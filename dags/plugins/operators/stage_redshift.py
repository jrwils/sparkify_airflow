from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 s3_bucket="",
                 s3_key="",
                 destination_table="",
                 redshift_conn_id='redshift',
                 aws_credentials_id='aws_credentials',
                 *args,
                 **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.destination_table = destination_table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id

    def _s3_file_path(self, execution_date):
        date_str = execution_date.strftime("%Y-%m-%d")
        s3_key = self.s3_key.format(date_str)
        s3_path = os.path.join('s3://', self.s3_bucket, s3_key)
        return s3_path

    def _aws_credentials(self):
        aws = AwsHook(self.aws_credentials_id, client_type='s3')
        return aws.get_credentials()

    def execute(self, context):
        s3_path = self._s3_file_path(context.get("execution_date"))
        credentials = self._aws_credentials()
        copy_sql = f"""
        COPY {self.destination_table} from '{s3_path}'
        ACCESS_KEY_ID '{credentials.access_key}'
        SECRET_ACCESS_KEY '{credentials.secret_key}'
        JSON 'auto ignorecase';
        """

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(copy_sql)
