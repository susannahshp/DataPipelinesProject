from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                table="",
                sql_query="",
                mode='append',
                *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.mode = mode

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.mode.lower() == 'truncate':
            self.log.info(f'Deleting data from {self.table} table & Loading data...')
            query = f"""
            TRUNCATE {self.table}; {self.sql_query}
            """
            redshift_hook.run(query)
            self.log.info(f"Dimension Table {self.table} loaded.")

        if self.mode.lower() == 'append':
            self.log.info(f'Appending data...')
            query = self.sql_query
            redshift_hook.run(query)
            self.log.info(f"Dimension Table {self.table} appended.")
            

