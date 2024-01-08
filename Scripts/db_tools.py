import pandas as pd
import psycopg2
from sqlalchemy import create_engine

def run_query (connection_details: dict, query: str) -> None:
    """
    Run a SQL query and return the results as a pandas DataFrame.
    """
    try:

        conn = psycopg2.connect(**connection_details)
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cursor.close()
        conn.close()
        return 1
    
# Inside fill_db() function
def fill_db(connection_details: dict, df: pd.DataFrame, table_name: str) -> None:
    engine = None
    try:
        db_url = f"postgresql+psycopg2://{connection_details['user']}:{connection_details['password']}@{connection_details['host']}:{connection_details['port']}/{connection_details['database']}"
        engine = create_engine(db_url, echo=False)
        print(f"DB URL: {db_url}")

        # Insert DataFrame into the database
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        print(f"Inserted {len(df)} rows into the database table {table_name}.")

        # Log information
        print(f"Inserted data:\n{df.head()}")

    except Exception as e:
        # Log the error
        print(f"Error inserting data into the database: {e}")

    finally:
        if engine:
            engine.dispose()




def create_table_query(df: pd.DataFrame, table_name: str) -> str:
    try:
        # Dictionary to map column names to PostgreSQL data types
        data_type_mapping = {
            'int64': 'INTEGER',
            'float64': 'DOUBLE PRECISION',
            'object': 'TEXT',
            'datetime64[ns]': 'DATE'  # Explicitly add datetime type
        }

        # Generate column definitions for the CREATE TABLE query
        column_definitions = ',\n    '.join([f'"{column}" {data_type_mapping.get(str(df[column].dtype), "TEXT")}' for column in df.columns])

        # Determine the primary key based on the data types
        primary_key_columns = ['"' + df.columns[0] + '"']

        if table_name in ['viewership_by_date_table_data', "totals_table_data"]:
            primary_key_columns = ['"' + df.columns[0] + '"']
        elif df.columns[0] == "Date":
            primary_key_columns.append('"' + df.columns[1] + '"')

        # Create the primary key constraint
        primary_key_constraint = f'PRIMARY KEY ({", ".join(primary_key_columns)})'

        # Create the full CREATE TABLE query
        create_table_query = f'''CREATE TABLE IF NOT EXISTS "{table_name}" ({column_definitions},{primary_key_constraint});'''
        
        return create_table_query

    except Exception as e:
        # Log the error
        print(f"Error creating table query: {e}")
        return ''

