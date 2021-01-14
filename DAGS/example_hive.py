from airflow import DAG
from airflow.operators import HiveOperator
from airflow.operators import HivePartitionSensor
from airflow.operators import DummyOperator

import re
import os
from datetime import datetime, timedelta
from hive_dynamic_query_config import config

'''
To Do:
'''

default_args = {
    'owner': 'sturaga',
    'email': ['tsuryasai@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2, #Total 3 attempts

}

user_defined_macros = {}

local_macros = {
    'local_hive_settings': """
        SET mapred.job.queue.name=prod-high;
    """,
}

user_defined_macros.update(local_macros)

def send_task_success(ds, **kwargs):
    print(ds)
    return 'Any custom operation, when this task completes'

# Read list of all the configs

PREFIX = "example_hive"

for i,config in enumerate(config):

    source = config["source"]

    # Dag Id should not have any non alphanumeric chars
    dag_id = '{}_{}'.format(PREFIX, config["name"])

    dag = DAG(dag_id=dag_id,
              default_args=default_args,
              user_defined_macros=user_defined_macros,
              schedule_interval=source["schedule_interval"],
              start_date=datetime.strptime(source["start_date"], '%Y-%m-%d')
              )

    # Create dependency tasks, In this example, we are waiting for another upstream partition
    dependency_list = []
    for dependency in source["dependencies"]:
        wait_for = HivePartitionSensor(
            task_id='{}_{}_{}'.format('wait_on_partition_', dependency["db"], dependency["table"]),
            dag=dag,
            table='{}.{}'.format(dependency["db"], dependency["table"]),
            partition=dependency["partition"], 
        )

        dependency_list.append(wait_for)

    #Create full path for the file
    hql_file_path = os.path.join(os.path.dirname(__file__), source['hql'])
    print(hql_file_path)
    run_hive_query = HiveOperator(
        task_id='run_hive_query',
        dag = dag,
        hql = """
        {{ local_hive_settings }}
        """ + "\n " + open(hql_file_path, 'r').read()
    )

    # dummy task
    all_tasks = DummyOperator(
        task_id='all_tasks',
        dag=dag,
        on_success_callback=send_task_success
    )

    # mark dependencies
    for dependency in dependency_list:
        dependency.set_downstream(run_hive_query)

    all_tasks.set_upstream(run_hive_query)


    #So that mulitple dags could be created
    # https://airflow.incubator.apache.org/faq.html#how-can-i-create-dags-dynamically
    globals()[dag_id] = dag
