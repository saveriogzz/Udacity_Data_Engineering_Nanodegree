from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import PostgresOperator
from airflow.operators.subdag_operator import SubDagOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                        LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from sparkify_dimensions_subdag import load_dim_tables_subdag


default_args = {
    'owner': 'Saverio Guzzo',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

parent_dag = 'Project5_DataPipeline_Airflow'

dag = DAG(parent_dag,
          start_date = datetime.today() - timedelta(days=2),
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs = 1
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    format_file="log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    load_fact_query=SqlQueries().songplay_table_insert
)

# load_user_dimension_table = LoadDimensionOperator(
#     task_id='Load_user_dim_table',
#     dag=dag,
#     redshift_conn_id="redshift",
#     load_dim_query=SqlQueries().users_table_insert
# )

# load_song_dimension_table = LoadDimensionOperator(
#     task_id='Load_song_dim_table',
#     dag=dag,
#     redshift_conn_id="redshift",
#     load_dim_query=SqlQueries().songs_table_insert
# )

# load_artist_dimension_table = LoadDimensionOperator(
#     task_id='Load_artist_dim_table',
#     dag=dag,
#     redshift_conn_id="redshift",
#     load_dim_query=SqlQueries().artists_table_insert
# )

# load_time_dimension_table = LoadDimensionOperator(
#     task_id='Load_time_dim_table',
#     dag=dag,
#     redshift_conn_id="redshift",
#     load_dim_query=SqlQueries().time_table_insert
# )

load_dim_table_tasks = SubDagOperator(
    task_id = 'Load_dim_tables_tasks',
    subdag=load_dim_tables_subdag(parent_dag,
                                  'Load_dim_tables_tasks',
                                  default_args),
    default_args=default_args,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables = ["artists", "songplays", "songs", "time", "users"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> load_dim_table_tasks >> run_quality_checks
run_quality_checks >> end_operator