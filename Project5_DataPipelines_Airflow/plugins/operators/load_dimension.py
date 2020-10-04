from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#11e6ed'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 load_dim_query="",
                 table="",
                 truncate=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.load_dim_query = load_dim_query
        self.truncate = truncate
        self.table = table

    def execute(self, context):  
        redshift = PostgresHook(self.redshift_conn_id)

        if self.truncate:
            self.log.info(f'-----> Truncating Dimensions table: {self.table}.')
            redshift.run(f"TRUNCATE TABLE {self.table}")

        formatted_query = self.load_dim_query.format(self.table)
        redshift.run(formatted_query)

        self.log.info(f'-----> Loading Dimensions table {self.table} completed.')
