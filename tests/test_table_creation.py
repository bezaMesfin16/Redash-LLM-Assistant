import pytest
import pandas as pd
import os
import sys
from dotenv import load_dotenv


current_dir = os.path.dirname(os.path.abspath(__file__))
# get the parent directory
project_root = os.path.abspath(os.path.join(current_dir, '..'))
# add the parent directory containing the module to the python path
sys.path.append(project_root)
from Scripts.db_tools import fill_db, run_query, create_table_query



load_dotenv()
# Mock connection details for testing purposes
connection_details = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'dbname': os.getenv('DB_NAME')
}

def test_run_query_success():
    # Test run_query with a valid query
    query = "SELECT * FROM your_table;"
    result = run_query(connection_details, query)
    assert result == 1

def test_run_query_failure():
    # Test run_query with an invalid query
    invalid_query = "SELECT * FROM non_existing_table;"
    result = run_query(connection_details, invalid_query)
    assert result == 1

def test_fill_db():
    # Create a sample DataFrame for testing
    data = {'Column1': [1, 2, 3], 'Column2': ['A', 'B', 'C']}
    df = pd.DataFrame(data)
    
    # Test fill_db function
    table_name = 'test_table'
    result = fill_db(connection_details, df, table_name)
    assert result is None

    # You can add more specific tests to check if the data is correctly inserted into the database.

def test_create_table_query():
    # Create a sample DataFrame for testing
    data = {'Column1': [1, 2, 3], 'Column2': ['A', 'B', 'C']}
    df = pd.DataFrame(data)

    # Test create_table_query function
    table_name = 'test_table'
    query = create_table_query(df, table_name)
    expected_query = '''CREATE TABLE IF NOT EXISTS "test_table" ("Column1" INTEGER,"Column2" TEXT,PRIMARY KEY ("Column1"));'''

    print(f"Generated Query:\n{query}")
    print(f"Expected Query:\n{expected_query}")

    # Compare queries by stripping whitespace and newlines
    assert query.strip() == expected_query.strip()



