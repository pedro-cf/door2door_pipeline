import psycopg2
import os
from contextlib import ContextDecorator


class PSQL_connect(ContextDecorator):
    """
    A context manager that creates a connection to a PostgreSQL database.

    Args:
        connection_string (str): Connection string to connect to the database. 
                                 If not provided, it defaults to the value of the 
                                 'PSQL_CONNECTION_STRING' environment variable.
        commit (bool): Whether to commit changes automatically on exit. Default is True.

    Example:
        with PSQL_connect() as (connection, cursor):
            cursor.execute('SELECT * FROM table_name')
            results = cursor.fetchall()
            ...
    """
    
    def __init__(
        self, connection_string=os.environ.get("PSQL_CONNECTION_STRING"), commit=True
    ):
        self.connection = psycopg2.connect(connection_string)
        if self.connection.status != psycopg2.extensions.STATUS_READY:
            raise Exception("Connection to the database is not working.")
        self.cursor = self.connection.cursor()
        self.commit = commit

    def __enter__(self):
        return self.connection, self.cursor

    def __exit__(self, *exc):
        if self.commit:
            self.connection.commit()
        self.cursor.close()
        self.connection.close()
        return False
