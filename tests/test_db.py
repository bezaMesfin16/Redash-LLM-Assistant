import pytest
import os
import sys
from dotenv import load_dotenv
from sqlalchemy import inspect

current_dir = os.path.dirname(os.path.abspath(__file__))
# get the parent directory
project_root = os.path.abspath(os.path.join(current_dir, '..'))
# add the parent directory containing the module to the python path
sys.path.append(project_root)

from Scripts.load_read_pg import CategorySchema


@pytest.fixture
def setup():
    load_dotenv()
    schema = CategorySchema()
    return schema

def test_connectDB():
    load_dotenv()
    schema = CategorySchema()
    engine = schema.connectDB()
    assert engine is not None

@pytest.fixture(scope="module")
def schema():
    load_dotenv()
    return CategorySchema()

@pytest.fixture(scope="module")
def engine(schema):
    return schema.connectDB()

def test_parse_and_insert_data(tmpdir):
    schema = CategorySchema()
    test_folder = tmpdir.mkdir("Test_Category")
    test_folder_path = str(test_folder)
    csv_path = os.path.join(test_folder_path, 'Chart data.csv')

    with open(csv_path, 'w') as file:
        file.write('Column1,Column2\nValue1,Value2\n')

    schema.parse_and_insert_data(test_folder_path, 'Test_Category')
    
    os.remove(csv_path)

def test_table_creation(tmpdir, schema, engine):
    # Create test folder and parse data
    test_folder = tmpdir.mkdir("Test_Category")
    test_folder_path = str(test_folder)
    schema.parse_and_insert_data(test_folder_path, 'Test_Category')

    # Get table names from the database
    inspector = inspect(engine)
    table_names = inspector.get_table_names()

    # Check if the table exists in the database
    assert 'Test_Category' in table_names