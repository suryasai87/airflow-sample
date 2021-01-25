from airflow import DAG
from airflow.operators import DummyOperator, PythonOperator
from datetime import datetime, timedelta
import airflow.hooks.S3_hook
import boto3
import requests

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 1, 1),
    'retry_delay': timedelta(minutes=5)
}

s3 = boto3.resource('s3')
'''
def upload_file_to_S3(filename, key, bucket_name):
    s3.Bucket(bucket_name).upload_file(filename, key)
'''

def upload_file_to_S3_with_hook(filename, key, bucket_name):
    hook = airflow.hooks.S3_hook.S3Hook('aws_default')
    hook.load_file(filename, key, bucket_name)


# Using the context manager alllows you not to duplicate the dag parameter in each operator
with DAG('ingestion_to_s3', default_args=default_args, schedule_interval='@once') as dag:

    start_task = DummyOperator(
            task_id='dummy_start'
    )



upload_to_S3_task = PythonOperator(
    task_id='upload_to_S3',
    python_callable=upload_file_to_S3_with_hook,
    op_kwargs={
        'filename': '\$AIRFLOW_HOME\state_codes.csv',
        'key': 'state_codes.csv',
        'bucket_name': 'sturaga-defloc',
    },
    dag=my_dag)
    
    
# Use arrows to set dependencies between tasks
start_task >> upload_to_S3_task
