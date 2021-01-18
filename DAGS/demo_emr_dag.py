#define the DAG
dag = airflow.DAG(
'demo_emr_dag',
schedule_interval='@once'
default_args=args,
max_active_runs=1)

default_emr_settings=()

create_job_flow_task = EMRCreateJobFlowOperator(
task_id='create_job_flow',
aws_conn_id = 'aws_default',
emr_conn_id='emr_default',
job_flow_overrides=default_emr_settings,
dag=dag
)

run_step = issue_step('load',["spark-submit","--deploy-mode","client","--master","yarn","--class","com.stripe.spark.examples.DiabetesQuery","/home/hadoop/first.jar"])

add_step_task = EMRAddStepsOperator(
task_id='add_step',
job_flow_id="((task_instance.xcom_pull('create_job_flow',key='return_value')))",
aws_conn_id='aws_default',
steps=run_step,
dag=dag
)

watch_prev_step_task = EMRStepSensor(
task_id='watch_prev_step',
job_flow_id="((task_instance.xcom_pull('add_step',key='return_value')[0]))",
aws_conn_id='aws_default',
dag=dag
)

terminate_job_flow_task = EMRTerminateJobFlowOperator(
task_id='terminate_job_flow',
job_flow_id="((task_instance.xcom_pull('create_job_flow',key='return_value')))",
aws_conn_id='aws_default',
trigger_rule="all_done",
dag=dag
)

#specifying the dependecncy structure
check_data_exists_task >> create_job_flow_task
create_job_flow_task >> add_step_task
add_step_task >> watch_prev_step_task
watch_prev_step_task >> terminate_job_flow_task
