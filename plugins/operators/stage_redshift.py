from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
    Operator used for loading the staging tables from json files.
    """
    
    ui_color = '#358140'    
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        {}
        ;
    """

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_prefix="",
                 region="us-west-2",
                 copy_options="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.region = region
        self.copy_options = copy_options

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Copying data from S3 to Redshift staging {self.table} table")
        s3_path = f"s3://{self.s3_bucket}/{self.s3_prefix}"

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.copy_options
        )
        
        self.log.info(f"Executing query to copy data from '{s3_path}' to '{self.table}'")
        redshift.run(formatted_sql)
