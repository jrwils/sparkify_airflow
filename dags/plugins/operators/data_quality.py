from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id='redshift',
        comparison_pairs=(),
        *args,
        **kwargs
    ):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.comparison_pairs = comparison_pairs

    def _extract_comparison_value(self, inp, execution_date, pg_hook):
        if isinstance(inp, int):
            return inp
        else:
            query = inp.format(execution_date=execution_date)
            pg_result = pg_hook.get_first(query)
            return pg_result[0]

    def execute(self, context):
        execution_date = context.get("execution_date")
        pg_hook = PostgresHook(self.redshift_conn_id)
        for pair in self.comparison_pairs:
            compare_val_1 = self._extract_comparison_value(
                pair[0],
                execution_date,
                pg_hook
            )
            compare_val_2 = self._extract_comparison_value(
                pair[1],
                execution_date,
                pg_hook
            )

            self.log.info(f"Execution Data {execution_date}")
            self.log.info("Running Data Quality Checks: ")
            for item in pair:
                self.log.info(item)
            self.log.info(f"Value 1 Comparison: {compare_val_1}")
            self.log.info(f"Value 2 Comparison: {compare_val_2}")
            assert compare_val_1 == compare_val_2
