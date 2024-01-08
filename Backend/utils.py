import pandas as pd
import psycopg2
import logging
from psycopg2 import OperationalError
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

class DatabaseManager:
    def __init__(self):
        self.connection_params = {
            "user": os.environ.get("DB_USER"),
            "password": os.environ.get("DB_PASSWORD"),
            "database": os.environ.get("DB_NAME"),
            "host": os.environ.get("DB_HOST"),
            "port": os.environ.get("DB_PORT")
        }
        self.logger = self.setup_logger()

    def setup_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        return logger

    def execute_query(self, query: str) -> tuple:
        """
        Execute a SQL query on a PostgreSQL database and return the results and cursor description.

        Parameters:
            query (str): SQL query to be executed.

        Returns:
            tuple: A tuple containing the results and cursor description.

        Raises:
            Exception: If there is an error during the execution or fetching of data.
        """
        conn = None
        cursor = None
        try:
            conn = psycopg2.connect(**self.connection_params)
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            description = cursor.description
            return results, description

        except OperationalError as e:
            self.logger.error(f"Operational error: {e}")
            raise Exception("Connection to the database failed.")

        except Exception as e:
            self.logger.error(f"Error executing query or fetching data: {e}")
            raise Exception("Error executing query or fetching data.")

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def execute_query_to_df(self, query: str) -> pd.DataFrame:
        """
        Execute a SQL query on a PostgreSQL database and return the results as a Pandas DataFrame.

        Parameters:
            query (str): SQL query to be executed.

        Returns:
            pd.DataFrame: The results of the query as a Pandas DataFrame.

        Raises:
            Exception: If there is an error during the execution or conversion of query results.
        """
        try:
            results, description = self.execute_query(query)
            columns = [desc[0] for desc in description]
            df = pd.DataFrame(results, columns=columns)
            return df

        except Exception as e:
            self.logger.error(f"Error converting query results to Pandas DataFrame: {e}")
            raise Exception("Error converting query results to Pandas DataFrame.")
