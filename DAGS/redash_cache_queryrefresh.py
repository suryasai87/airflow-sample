'''
from __future__ import absolute_import, unicode_literals, print_function
import os
import logging
import airflow
from airflow import DAG
from airflow.operators import BashOperator, HiveOperator
from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator

# TODO: add timeout
while job['status'] not in (3,4):
    response = s.get('{}/api/jobs/{}'.format(redash_url, job['id']))
    job = response.json()['job']
    time.sleep(1)

if job['status'] == 3:
    return job['query_result_id']

return None

payload = dict(max_age = 0, parameters = params)

response = s.post('{}/api/queries/{}/results'.format(redash_url, query_id), data = json.dumps(payload))

if response.status_code != 200:
    raise Exception('Refresh failed.')

result_id = poll_job(s, redash_url, response.json()['job'])

if result_id:
    response = s.get('{}/api/queries/{}/results/{}.json'.format(redash_url, query_id, result_id))
    if response.status_code != 200:
        raise Exception('Failed getting results.')
else:
    raise Exception('Query execution failed.')

return response.json()['query_result']['data']['rows']
'''
