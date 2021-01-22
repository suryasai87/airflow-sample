from __future__ import absolute_import, unicode_literals, print_function
import os
import logging
import airflow
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
from airflow.contrib.operators.qubole_sensors import QuboleFileSensor, QubolePartitionSensor
import filecmp

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
    dag_id='data_pipeline',
    default_args=default_args,
    schedule_interval="15 08 * * *",
    dagrun_timeout=timedelta(minutes=1),
    max_active_runs=1)

sqoop_job = """
 exec ./scripts/sqoop_incremental.sh
"""
# Importing the data from Mysql table to HDFS
sqoop_import_task = BashOperator(
        task_id= 'sqoop_import',
        bash_command=sqoop_job,
        dag=dag
)

# Inserting the data from Hive external table to the target table
hive_create_ddl_task = HiveOperator(
        task_id= 'hive_create_ddl',
        hql='drop table if exists `default.prescription_drug_events_2008_2010`;
create external table if not exists `default.prescription_drug_events_2008_2010`(
DESYNPUF_ID STRING
,PDE_ID STRING
,SRVC_DT STRING
,PROD_SRVC_ID STRING
,QTY_DSPNSD_NUM STRING
,DAYS_SUPLY_NUM STRING
,PTNT_PAY_AMT STRING
,TOT_RX_CST_AMT STRING
)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
LOCATION
  's3a://synpuf/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_1'
TBLPROPERTIES ('skip.header.line.count'='1'); ',
        depends_on_past=True,
        dag=dag
)

commands = ['INSERT INTO TABLE orders_trans SELECT order_id, first_name,last_name, item_code, order_date FROM orders_stg;'
             ,'cmd2','...'
           ]

for sqlcmd in commands:
        ret = HiveOperator(
            task_id='hive_data_blending',
            hiveconf_jinja_translate=True,
            hql=sqlcmd,
            trigger_rule='all_done',
            dag=dag
        )

# https://docs.aws.amazon.com/emr/latest/APIReference/API_RunJobFlow.html
default_emr_settings = {"Name": "finance_job_flow",
                        "LogUri": "s3://sturaga-defloc/firstfolder/logs/",
                        "ReleaseLabel": "emr-5.19.0",
                        "Instances": {
                            "InstanceGroups": [
                                {
                                    "Name": "Master nodes",
                                    "Market": "ON_DEMAND",
                                    "InstanceRole": "MASTER",
                                    "InstanceType": "m5.xlarge",
                                    "InstanceCount": 1
                                },
                                {
                                    "Name": "Worker nodes",
                                    "Market": "ON_DEMAND",
                                    "InstanceRole": "CORE",
                                    "InstanceType": "m5.xlarge",
                                    "InstanceCount": 1
                                }
                            ],
                            "Ec2KeyName": "sturaga",
                            "KeepJobFlowAliveWhenNoSteps": True,
                            'EmrManagedMasterSecurityGroup': 'sg-0b705a418dec3329e',
                            'EmrManagedSlaveSecurityGroup': 'sg-0b705a418dec3329e',
                            'Placement': {
                                'AvailabilityZone': 'us-gov-east-1a',
                            },

                        },
                        "BootstrapActions": [
                            {
                                'Name': 'copy jar to local',
                                'ScriptBootstrapAction': {
                                    'Path': 's3://sturaga-defloc/firstfolder/bootstrap_script/bootstrap_cluster.sh'
                                }
                            }
                        ],

                        "Applications": [
                            {"Name": "Spark"}
                        ],
                        "VisibleToAllUsers": True,
                        "JobFlowRole": "EMR_EC2_DefaultRole",
                        "ServiceRole": "EMR_DefaultRole",
                        "Tags": [
                            {
                                "Key": "app",
                                "Value": "analytics"
                            },
                            {
                                "Key": "environment",
                                "Value": "development"
                            }
                        ]
                        }
def issue_step(name, args):
    return [
        {
            "Name": name,
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": default_args
            }
        }
    ]


def check_data_exists():
    logging.info('checking that data exists in s3')
    source_s3 = S3Hook(aws_conn_id='aws_default')
    keys = source_s3.list_keys(bucket_name='synpuf',
                               prefix='DE1_0_2008_Beneficiary_Summary_File_Sample_1/')
    logging.info('keys {}'.format(keys))


check_data_exists_task = PythonOperator(task_id='check_data_exists',
                                        python_callable=check_data_exists,
                                        provide_context=False,
                                        dag=dag)

create_job_flow_task = EmrCreateJobFlowOperator(
    task_id='create_job_flow',
    aws_conn_id='aws_default',
    emr_conn_id='emr_default',
    job_flow_overrides=default_emr_settings,
    dag=dag
)

run_step = issue_step('run_spark_jar', ["spark-submit", "--deploy-mode", "client", "--master",
                               "yarn", "--class", "org.apache.spark.examples.JavaLogQuery",
                               "/home/hadoop/one.jar"])

add_step_task = EmrAddStepsOperator(
    task_id='add_step',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=run_step,
    dag=dag
)

watch_prev_step_task = EmrStepSensor(
    task_id='watch_prev_step',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull('add_step', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)

terminate_job_flow_task = EmrTerminateJobFlowOperator(
    task_id='terminate_job_flow',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    aws_conn_id='aws_default',
    trigger_rule="all_done",
    dag=dag
)

def compare_result(ds, **kwargs):
    ti = kwargs['ti']
    r1 = t1.get_results(ti)
    r2 = t2.get_results(ti)
    return filecmp.cmp(r1, r2)

source_schema_changes_task = PythonOperator(
    task_id='check_for_source_schema_changes',
    provide_context=True,
    python_callable=compare_result,
    dag=dag)

options = ['shell_cmd', 'presto_cmd', 'db_query', 'spark_cmd']
branching = BranchPythonOperator(
    task_id='branching',
    python_callable=lambda: options[0],
    trigger_rule="one_success",
    dag=dag)
branching.set_upstream(source_schema_changes_task)

join = DummyOperator(
    task_id='join',
    trigger_rule="one_success",
    dag=dag
)

# defining the job dependency
sqoop_import_task >> hive_create_ddl_task
hive_create_ddl_task >> check_data_exists_task
#check_data_exists_task >> hive_data_blending
#hive_data_blending >> create_job_flow_task

check_data_exists_task >> create_job_flow_task

create_job_flow_task >> add_step_task
add_step_task >> watch_prev_step_task
watch_prev_step_task >> terminate_job_flow_task
