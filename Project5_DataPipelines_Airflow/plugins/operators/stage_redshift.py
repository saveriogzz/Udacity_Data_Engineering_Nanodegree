from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    
    ui_color = '#358140'

    # template_fields = (" ")
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS json '{}';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 format_file="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.format_file = format_file

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('------> Truncating entries from {}.'.format(self.table))
        redshift.run("TRUNCATE {};".format(self.table))
    
        self.log.info('------> Copying data from S3 to Redshift.')
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)

        if self.format_file != '':
            formatter = "s3://{}/{}".format(self.s3_bucket, self.format_file)
        else:
            formatter = 'auto'

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key, credentials.secret_key,
                formatter
            )

        redshift.run(formatted_sql)

        self.log.info(f"------> Table {self.table} staged.")

