from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Operator used for loading the fact table from the staging tables.
    the parameters are:
    table-> the name of the fact table
    redshift_conn_id-> was redshift connection details
    load_sql_stmt->the select statement for loading the data into the fact table
    """
    ui_color = '#F98866'

    insert_sql = """
        INSERT INTO {}
        {};
    """
    
    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 load_sql_stmt="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.load_sql_stmt = load_sql_stmt

    def execute(self, context):
        self.log.info('executing LoadFactOperator')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        formatted_sql = LoadFactOperator.insert_sql.format(
            self.table,
            self.load_sql_stmt
        )
        redshift.run(formatted_sql)
