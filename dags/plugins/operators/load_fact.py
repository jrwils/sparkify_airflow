from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id='redshift',
        sql_data='',
        destination_table='',
        *args,
        **kwargs
    ):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.sql_data = sql_data

    def execute(self, context):
        pg_hook = PostgresHook(self.redshift_conn_id)
        execution_date = context.get("execution_date")
        sql_data = self.sql_data.format(execution_date=execution_date)
        self.log.info('sql_data')
        self.log.info(sql_data)
        sql_str = f"""
        INSERT into {self.destination_table} (
            {sql_data}
        )
        """
        pg_hook.run(sql_str)
