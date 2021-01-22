'''
"""
Airflow Demo leveraging QuboleOperator
"""

from airflow import DAG
from airflow.operators import BashOperator, HiveOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.operators.qubole_operator import QuboleOperator
from airflow.contrib.operators.qubole_sensors import QuboleFileSensor, QubolePartitionSensor
import filecmp

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': ['test@domain.com'],
    'email_on_failure': True,
    'email_on_retry': False
}
dag = DAG('airflow-demo', default_args=default_args, schedule_interval='@hourly')

dag.doc_md = __doc__

def compare_result(ds, **kwargs):
    ti = kwargs['ti']
    r1 = t1.get_results(ti)
    r2 = t2.get_results(ti)
    return filecmp.cmp(r1, r2)

t1 = HiveOperator(
    task_id='hive_show_table',
    command_type='hivecmd',
    query='show tables',
    cluster_label='default',
    tags='aiflow_example_run',  
    hive_conn_id='hive_default',  
    dag=dag)

t2 = HiveOperator(
    task_id='hive_query_from_s3_location',
    command_type="hivecmd",
    script_location="s3a://public-dataset/library/scripts/show_table.hql",
    notfiy=True,
    tags=['tag1', 'tag2'],
    dag=dag)

t3 = PythonOperator(
    task_id='compare_result',
    provide_context=True,
    python_callable=compare_result,
    dag=dag)

t3.set_upstream(t1)
t3.set_upstream(t2)

options = ['shell_cmd', 'presto_cmd', 'db_query', 'spark_cmd']

branching = BranchPythonOperator(
    task_id='branching',
    python_callable=lambda: options[0],
    trigger_rule="one_success",
    dag=dag)
branching.set_upstream(t3)

join = DummyOperator(
    task_id='join',
    trigger_rule="one_success",
    dag=dag
)

t4 = QuboleOperator(
    task_id='shell_cmd',
    command_type="shellcmd",
    script_location="s3://public-dataset/library/scripts/shell.sh",
    parameters="param1 param2",
    dag=dag)

t5 = QuboleOperator(
    task_id='pig_cmd',
    command_type="pigcmd",
    script_location="s3://public-dataset/library/scripts/script1-hadoop-s3-small.pig",
    parameters="key1=value1 key2=value2",
    dag=dag)

t4.set_upstream(branching)
t5.set_upstream(t4)
t5.set_downstream(join)

t6 = BashOperator(
    task_id='presto_cmd',
    command_type='prestocmd',
    query='show tables',
    dag=dag)

t7 = HiveOperator(
    task_id='hadoop_jar_cmd',
    command_type='hadoopcmd',
    sub_command='jar s3://dataset/HadoopAPIExamples/jars/hadoop-0.20.1-dev-streaming.jar -mapper wc -numReduceTasks 0 -input s3://dataset/HadoopAPITests/data/3.tsv -output s3://datasete/HadoopAPITests/data/3_wc',
    cluster_label='default',
    dag=dag)

t6.set_upstream(branching)
t7.set_upstream(t6)
t7.set_downstream(join)

t8 = HiveOperator(
    task_id='db_query',
    command_type='mssqlcmd',
    query='show tables',
    dag=dag)

t9 = HiveOperator(
    task_id='db_export',
    command_type='dbexportcmd',
    mode=1,
    hive_table='default__airline_origin_destination',
    db_table='exported_airline_origin_destination',
    partition_spec='dt=20110104-02',
    dbtap_id=2064,
    dag=dag)

t8.set_upstream(branching)
t9.set_upstream(t8)
t9.set_downstream(join)

t10 = HiveOperator(
    task_id='db_import',
    command_type='dbimportcmd',
    mode=1,
    hive_table='default_airline_origin_destination',
    db_table='exported_airline_origin_destination',
    where_clause='id < 10',
    db_parallelism=2,
    dbtap_id=2064,
    dag=dag)
''''''
prog = '''
'''

import scala.math.random

import org.apache.spark._

/** Computes an approximation to pi */
object SparkPi {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi")
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop()
  }
}
'''
'''
t11 = BashOperator(
    task_id='spark_cmd',
    command_type="sparkcmd",
    program=prog,
    language='scala',
    arguments='--class SparkPi',
    tags='aiflow_example_run',
    dag=dag)

t11.set_upstream(branching)
t11.set_downstream(t10)
t10.set_downstream(join)


t12 = QuboleFileSensor(
    task_id='check_s3_file',
    qubole_conn_id='qubole_default',
    poke_interval=10,
    timeout=60,
    data={"files":
              ["s3://paid-qubole/HadoopAPIExamples/jars/hadoop-0.20.1-dev-streaming.jar",
              "s3://paid-qubole/HadoopAPITests/data/3.tsv"

               ] # will check for availability of all the files in array
          },
    dag=dag
    )

t13 = QubolePartitionSensor(
    task_id='check_hive_partition',
    poke_interval=10,
    timeout=60,
    data={"schema":"default",
          "table":"my_partitioned_table",
          "columns":[
              {"column" : "month", "values" : ["{{ ds.split('-')[1] }}"]},
              {"column" : "day", "values" : ["{{ ds.split('-')[2] }}" , "{{ yesterday_ds.split('-')[2] }}"]}
            ]# will check for partitions like [month=12/day=12,month=12/day=13]
          },
    dag=dag
    )

alert = EmailOperator(
    task_id='CompletionEmail',
    to='test@domain.com',
    subject='Airflow processing report',
    html_content='raw content #2',
    dag=dag
)
join.set_downstream(t12)
t12.set_downstream(alert)
'''
