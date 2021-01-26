''''
!pip install requests
!pip install s3fs
!pip install pandas
''''

''''
from __future__ import absolute_import, unicode_literals, print_function

from airflow import DAG
from airflow.operators import BashOperator, HiveOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.models.connection import Connection

import os
import logging
import airflow
import filecmp
import requests
import pandas as pd

os.environ["AWS_ACCESS_KEY_ID"] = blaine_aws_conn.AWS_ACCESS_KEY_ID
os.environ["AWS_SECRET_ACCESS_KEY"] = blaine_aws_conn.AWS_SECRET_ACCESS_KEY
os.environ["AWS_DEFAULT_REGION"] = blaine_aws_conn.AWS_DEFAULT_REGION

_keys={AWS_ACCESS_KEY_ID: ""
      }  

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 1, 19),
    'depends_on_past': False,
    'retries': 1,
    'provide_context': True,
    'retry_delay': timedelta(minutes=5),
    'email': ['test@domain.com'],
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG(
    dag_id='python_workflow',
    default_args=default_args,
    schedule_interval="15 08 * * *",
    dagrun_timeout=timedelta(minutes=1),
    max_active_runs=1)
    
  
    
os.environ["AWS_ACCESS_KEY_ID"] = blaine_aws_conn.AWS_ACCESS_KEY_ID
os.environ["AWS_SECRET_ACCESS_KEY"] = blaine_aws_conn.AWS_SECRET_ACCESS_KEY
os.environ["AWS_DEFAULT_REGION"] = blaine_aws_conn.AWS_DEFAULT_REGION
#

#
key = blaine_census_API_key.key
#
# parse out the data from Census API
#
def parse_query(json_text):
    """

    :param json_text: json formatted text data from Census API
    :type json_text: json
    :return: DataFrame with parsed data
    :rtype: pandas.DataFrame()

    """
    data = pd.read_json(json_text).iloc[1:, :]
    data.columns = pd.read_json(json_text).iloc[0, :]
#
    return data
#
# call census API for FIPS states and counties
#
year = '2010'
url = 'https://api.census.gov/data/' + year + '/dec/sf1'
FIPS_states_query = requests.get(url + '?get=NAME&for=state:*' + '&key=' + key)
FIPS_states_table = parse_query(FIPS_states_query.text)
#
FIPS_states_table.to_csv('s3://synpuf/state_codes/FIPS_states_table.csv')
''''

