import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy

class CategorySchema:
    def __init__(self):
        load_dotenv()

    def connectDB(self):
        host = os.getenv('DB_HOST')
        port = os.getenv('DB_PORT')
        dbname = os.getenv('DB_NAME')
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        engine = sqlalchemy.create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")
        return engine

    def sanitize_table_name(self, table_name):
        # Implement logic to sanitize table names if needed
        # Ensure compliance with PostgreSQL naming conventions

        # Example: Replace spaces with underscores
        return table_name.replace(' ', '_')

    def parse_and_insert_data(self, folder_path, folder_name):
        engine = self.connectDB()
        table_name = self.sanitize_table_name(folder_name)
        csv_file_path = f"{folder_path}/{folder_name}/Chart data.csv"

        if os.path.exists(csv_file_path):
            try:
                df = pd.read_csv(csv_file_path)
                df.columns = df.columns.str.replace(' ', '_')

                # Insert data into the database
                df.to_sql(table_name, engine, if_exists='append', index=False)
                print(f"Data inserted into table '{table_name}' successfully.")
            except Exception as e:
                print(f"Error occurred while inserting data into table '{table_name}': {e}")

if __name__ == "__main__":
    data_directory = '/home/biniyam/Redash-LLM-Assistant/Data'

    schema = CategorySchema()
    for folder_name in os.listdir(data_directory):
        folder_path = os.path.join(data_directory, folder_name)
        if os.path.isdir(folder_path):
            schema.parse_and_insert_data(folder_path, folder_name)
