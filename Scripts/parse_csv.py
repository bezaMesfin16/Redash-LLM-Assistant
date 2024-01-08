#parse csv to database tables
import csv
import pandas as pd
import psycopg2
import dotenv
import os

dotenv.load_dotenv()


conn = psycopg2.connect(
    dbname= os.getenv("DB_NAME"),
    user= os.getenv("DB_USER"),
    password= os.getenv("DB_PASSWORD"),
    host= os.getenv("DB_HOST"),
    port= os.getenv("DB_PORT")

)

table_names = [
    'Cities', 'Geography', 'Sharing service', 'Subtitles and CC',
    'Viewer gender', 'Content type', 'New and returning viewers',
    'Subscription source', 'Traffic source', 'Viewership by Date',
    'Device type', 'Operating system', 'Subscription status', 'Viewer age'
]

base_path = '/path/to/Redash-LLM-Assistant/Data/'

for table_name in table_names:
    for filename in ['chart_data.csv', 'table_data.csv', 'total.csv']:
        file_path = f"{base_path}{table_name}/{filename}"
        try:
            df = pd.read_csv(file_path)
            df.to_sql(table_name, conn, if_exists='append', index=False)
            print(f"Data from {file_path} loaded into table {table_name}")
        except FileNotFoundError:
            print(f"File {file_path} not found")

conn.close() 
