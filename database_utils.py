import pandas as pd
from sqlalchemy import create_engine

class DatabaseConnector:
    """A utility class to connect to a database and upload/query data."""

    def __init__(self, db_url):
        """Initialises the database connection using a connection URL."""
        self.engine = create_engine(db_url)

    def upload_to_db(self, df, table_name):
        """Uploads a DataFrame to a database table."""
        df.to_sql(table_name, self.engine, if_exists='replace', index=False)

    def query_db(self, query):
        """Executes an SQL query and returns the results as a DataFrame."""
        with self.engine.connect() as conn:
            return pd.read_sql(query, conn)
