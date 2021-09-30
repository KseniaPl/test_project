
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from scripts.hive_task import hive_task
from scripts.hdfs_task import hdfs_task
from scripts.pass_op import pass_op


now = datetime.now()
now_to_the_hour = (now - timedelta(0, 0, 0, 0, 0, 3)).replace(minute=0, second=0, microsecond=0)
START_DATE = now_to_the_hour
DAG_NAME = 'etl_dag'

default_args = {'owner': 'airflow', 'depends_on_past': True, 'start_date': days_ago(2)}
dag = DAG(DAG_NAME, schedule_interval='@daily', default_args=default_args)


t1 = PythonOperator(
	task_id='copy_from_hdfs',
	python_callable=hdfs_task,
	dag=dag
)

t2 = PythonOperator(
	task_id='kafka_producer',
	python_callable=pass_op,
	dag=dag
)
t3 = PythonOperator(
	task_id='kafka_receiver',
	python_callable=pass_op,
	dag=dag
)
t4 = PythonOperator(
	task_id='run_hive',
	python_callable=hive_task,
	dag=dag
)

t1>>t2>>t3>>t4
