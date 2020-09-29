from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 load_dim_query="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.load_dim_query = load_dim_query


    def execute(self, context):
        self.log.info('-----> Loading Dimensions table.')
        redshift = PostgresHook(self.redshift_conn_id)
        redshift.run(self.load_dim_query)

        self.log.info('-----> Loading Dimensions table completed.')

