#File: data_extraction.py
class DataExtractor:
    def __init__(self):
        pass
    def extract_data_from_file(self, file_path):
        pass
    def extract_data_from_database(self, connection, query):
        pass
    def extract_data_from_api(self, api_endpoint):
        pass

#File: database_utils.py
import sqlite3
class DatabaseConnector:
    def __init__(self, db_name):
        self.db_name = db_name
        self.connection = None 
    def connect_to_database(self):
        pass
#File: data_cleaning.py
import pandas as pd 
class DataCleaning:
    def __init__(self):
        pass 
    def clean_file_data(self,data):
        pass
    def clean_database_data(self.data): 
        pass 
    def clean_api_data(self,data):
        
        pass 
#File: database_utils.py
import yaml
with open('db_creds.yaml', 'r') as file:
    db_creds = yaml.safe_load(file)

print(db_creds)

import psycopg2
class DatabaseConnector:
    def __init__(self, creds_file):
        self.creds_file = creds_file
        self.connection = None
    def load_credentials(self):
        with open(self.creds_file, 'r') as file: creds = yaml.safe_load(file)
        return creds
    def connect_to_database(self):
        try:
            creds = self.load_credentials()
            self.connection = psycopg2.connect(host = creds['RDS_HOST'], database = creds['RDS_DATABASE'], user = creds['RDS_USER'], password = creds['RDS_PASSWORD'], port = creds['RDS_PORT'])
            print("Connected to the AWS RDS database.")
            return self.connection
        except psycopg2.Error as e:
            print(f"An error occurred while connecting to the database: {e}")
            return None 
    def upload_data_to_database(self, table_name, data):
        try:
            cursor = self.connection.cursor()
            insert_query = f"INSERT INTO {table_name} VALUES (%s, %s, %s)"
            cursor.executemany(insert_query, data) 
            self.connection.commit()
            print(f"Data successfully uploaded to {table_name}")
        except psycopg2.Error as e:
            print(f"An error occured while uploading data to the database:{e}")
        def close_connection(self):
            if self.connection: self.connection.close()
            print("Database connection closed") 
#File: data_extraction.py
from database_utils import DatabaseConnector 
class DataExtractor:
    def __init__(self, db_connector): self.db_connector = db_connector
    def extract_data_from_database(self, query):
        try:
            connection = self.db_connector.connect_to_database()
            if connection:
                cursor = connection.cursor()
                cursor.executive(query)
                data = cursor.fetchall()
                return data 
        except Exception as e:
            print(f"An error occured while querying the database: {e}")
            return None
        finally:
            self.db_connector.close_connection()
import yaml

def read_db_creds(file_path='db_creds.yaml'):
    try:
        with open(file_path, 'r') as file:
            db_creds = yaml.safe_load(file)
            return db_creds
    except FileNotFoundError:
        print(f"Error: The file {file_path} was not found.")
        return None
    except yaml.YAMLError as exc:
        print(f"Error parsing YAML file: {exc}")
        return None
from sqlalchemy import create_engine

def init_db_engine(credentials):
    try:
        db_url = (
            f"postgresql://{credentials['RDS_USER']}:{credentials['RDS_PASSWORD']}@"
            f"{credentials['RDS_HOST']}:{credentials['RDS_PORT']}/{credentials['RDS_DATABASE']}"
        )
        engine = create_engine(db_url)
        return engine

    except KeyError as e:
        print(f"Error: Missing required database credential - {e}")
        return None
    except Exception as exc:
        print(f"Error initializing database engine: {exc}")
        return None
from sqlalchemy import create_engine, inspect
import pandas as pd

class DataExtractor:
    def __init__(self, engine):
        self.engine = engine

    def list_db_tables(self):
        try:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()
            return tables
        except Exception as exc:
            print(f"Error listing tables: {exc}")
            return None

    def read_table_data(self, table_name):
        try:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, self.engine)
            return df
        except Exception as exc:
            print(f"Error reading data from table {table_name}: {exc}")
            return None
class DataExtractor:
    def __init__(self, engine):
        self.engine = engine

    def list_db_tables(self):
        try:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()
            return tables
        except Exception as exc:
            print(f"Error listing tables: {exc}")
            return None

    def read_table_data(self, table_name):
        try:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, self.engine)
            return df
        except Exception as exc:
            print(f"Error reading data from table {table_name}: {exc}")
            return None

    def read_rds_table(self, db_connector, table_name):
        try:
            engine = db_connector.get_engine()
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, engine)
            return df
        except Exception as exc:
            print(f"Error reading data from table {table_name}: {exc}")
            return None
class DatabaseConnector:
    def __init__(self, credentials):
        self.credentials = credentials

    def get_engine(self):
        try:
            db_url = (
                f"postgresql://{self.credentials['RDS_USER']}:{self.credentials['RDS_PASSWORD']}@"
                f"{self.credentials['RDS_HOST']}:{self.credentials['RDS_PORT']}/{self.credentials['RDS_DATABASE']}"
            )
            engine = create_engine(db_url)
            return engine
        except KeyError as e:
            print(f"Error: Missing required database credential - {e}")
            return None
        except Exception as exc:
            print(f"Error initializing database engine: {exc}")
            return None
import pandas as pd
import numpy as np

class DataCleaning:
    def clean_user_data(self, df):
        # Handle NULL values
        df = self._handle_null_values(df)

        # Fix date errors
        df = self._fix_date_errors(df)

        # Correct incorrectly typed values
        df = self._correct_data_types(df)

        # Remove rows with inappropriate or wrong information
        df = self._remove_invalid_rows(df)

        return df

    def _handle_null_values(self, df):
        # Example strategy: drop rows where essential columns are NULL
        df = df.dropna(subset=['user_id', 'email'])  # Drop rows where essential columns are NULL
        df = df.fillna({'age': df['age'].median()})  # Fill NULL age with median age
        return df

    def _fix_date_errors(self, df):
        # Example: Ensure 'signup_date' is a datetime type and correct format issues
        df['signup_date'] = pd.to_datetime(df['signup_date'], errors='coerce')

        # Drop rows where 'signup_date' is still NULL after conversion
        df = df.dropna(subset=['signup_date'])

        # Example: Filter out dates that are unrealistic (e.g., in the future)
        current_date = pd.Timestamp.now()
        df = df[df['signup_date'] <= current_date]

        return df

    def _correct_data_types(self, df):
        # Example: Ensure 'age' is an integer
        df['age'] = pd.to_numeric(df['age'], errors='coerce')

        # Drop rows where 'age' is not a positive integer
        df = df[(df['age'] > 0) & (df['age'].notnull())]

        return df

    def _remove_invalid_rows(self, df):
        # Example: Remove rows where 'email' is invalid
        df = df[df['email'].str.contains('@')]

        # Example: Remove duplicate rows based on 'user_id'
        df = df.drop_duplicates(subset=['user_id'])

        return df
class DatabaseConnector:
    def __init__(self, credentials):
        self.credentials = credentials

    def get_engine(self):
        try:
            db_url = (
                f"postgresql://{self.credentials['RDS_USER']}:{self.credentials['RDS_PASSWORD']}@"
                f"{self.credentials['RDS_HOST']}:{self.credentials['RDS_PORT']}/{self.credentials['RDS_DATABASE']}"
            )
            engine = create_engine(db_url)
            return engine
        except KeyError as e:
            print(f"Error: Missing required database credential - {e}")
            return None
        except Exception as exc:
            print(f"Error initializing database engine: {exc}")
            return None

    def upload_to_db(self, df, table_name):
        try:
            engine = self.get_engine()
            if engine is not None:
                df.to_sql(table_name, engine, if_exists='replace', index=False)
                print(f"Data uploaded successfully to table '{table_name}'.")
            else:
                print("Failed to upload data: engine initialization failed.")
        except Exception as exc:
            print(f"Error uploading data to table {table_name}: {exc}")

creds = read_db_creds('db_creds.yml')
if creds:
    db_connector = DatabaseConnector(creds)
    extractor = DataExtractor(db_connector.get_engine())
    tables = extractor.list_db_tables()
    
    if tables:
        user_data_table = next((table for table in tables if 'user' in table.lower()), None)
        
        if user_data_table:
            print(f"Extracting data from table: {user_data_table}")
            user_df = extractor.read_rds_table(db_connector, user_data_table)
            
            if user_df is not None:
                print("User data extracted successfully.")
            
                cleaner = DataCleaning()
                cleaned_user_df = cleaner.clean_user_data(user_df)
                print("User data cleaned successfully.")

                db_connector.upload_to_db(cleaned_user_df, 'dim_users')
            else:
                print("Failed to extract user data.")
        else:
            print("No user data table found in the database.")
    else:
        print("No tables found in the database.")
else:
    print("Failed to initialize the DatabaseConnector.")
