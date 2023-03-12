import logging
import os

from utils.psql_utils import PSQL_connect
from utils.utils import generate_correlation_id

def ensure_table_creation(**context):
    """Create necessary tables in PostgresSQL"""
    correlation_id = generate_correlation_id(context['dag_run'].run_id)
    logging.info(f"Running ensure_table_creation for {correlation_id=}")
    
    with PSQL_connect() as (conn, cur):
        # get a list of all SQL files in the directory
        sql_files = [f for f in os.listdir('/opt/airflow/resources/psql_tables') if f.endswith('.sql')]
        
        # iterate over the files and execute their contents
        for sql_file in sql_files:
            # open the file and read its contents
            with open(os.path.join('/opt/airflow/resources/psql_tables', sql_file), 'r') as f:
                script = f.read()
                # Create  table
                cur.execute(script)
