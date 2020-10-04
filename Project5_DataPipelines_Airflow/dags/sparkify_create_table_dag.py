from datetime import datetime, timedelta
import os
from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from operators import CreateTableOperator


default_args = {
    'owner': 'Saverio Guzzo',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 3),
    'email_on_failure': False,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG('Project5_CreateTables_Airflow',
          default_args=default_args,
          description='Create Tables in Redshift DWH with Airflow',
          max_active_runs = 1
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = CreateTableOperator(
    task_id="Create_tables",
    dag=dag,
    redshift_conn_id="redshift",
    query = open('../create_tables.sql', 'r').read()
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables >> end_operator