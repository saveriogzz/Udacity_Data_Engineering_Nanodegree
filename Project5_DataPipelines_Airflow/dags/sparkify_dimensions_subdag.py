from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import LoadDimensionOperator
from helpers import SqlQueries


def load_dim_tables_subdag(parent_dag_name,
                           child_dag_name,
                           args):

    global dim_tables, dim_queries
    dim_tables = ["artists", "songs", "time", "users"]
    dim_queries = SqlQueries()

    dag_subdag = DAG(f'{parent_dag_name}.{child_dag_name}',
                     start_date = datetime.today() - timedelta(days=2),
                     default_args=args
    )

    with dag_subdag:
        for dim_table in dim_tables:
            t = LoadDimensionOperator(
                task_id=f'Load_{dim_table}_dim_table',
                dag=dag_subdag,
                redshift_conn_id="redshift_conn_id",
                load_dim_query=vars(dim_queries)[f'{dim_table}_table_insert'],
                table=dim_table,
                truncate=True
            )

    return dag_subdag
