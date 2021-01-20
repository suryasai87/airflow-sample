from __future__ import absolute_import, unicode_literals
import os
from airflow import DAG
from airflow.operators import HiveOperator
from datetime import datetime, timedelta

args = {
    'owner': 'sturaga',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 19),
}

commands = ['cmd1','cmd2','...']
dag = DAG(
    dag_id='hive_demo',
    default_args=args,
    schedule_interval="15 08 * * *",
    dagrun_timeout=timedelta(minutes=1))

for sqlcmd in commands:

        ret = HiveOperator(
            task_id='hive_operator',
            hiveconf_jinja_translate=True,
            hql=sqlcmd,
            trigger_rule='all_done',
            dag=dag
        )
