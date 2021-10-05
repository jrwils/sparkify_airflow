from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 sql_data='',
                 destination_table='',
                 *args,
                 **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_data = sql_data
        self.destination_table = destination_table

    def execute(self, context):
        pg_hook = PostgresHook(self.redshift_conn_id)
        sql_str = f"""
        TRUNCATE {self.destination_table};
        INSERT into {self.destination_table} (
            {self.sql_data}
        )
        """
        pg_hook.run(sql_str)
