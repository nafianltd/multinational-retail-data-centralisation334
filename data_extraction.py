import pandas as pd
import tabula as tb
import requests
import json
import boto3
from database_utils import DatabaseConnector  # Import the connector

class DataExtractor:
    def __init__(self):
        pass

    def find_user_table(self, db_connector):
        """
        Finds the table containing user data.
        """
        engine = db_connector.init_db_engine(db_connector.read_db_creds())
        table_names = db_connector.list_db_tables(engine)

        # Print all tables to help identify the correct one
        print(f"📌 Available tables: {table_names}")

        # Assuming the user table contains 'user' in its name
        for table in table_names:
            if 'user' in table.lower():
                print(f"✅ Found user table: {table}")
                return table
        
        print("❌ No user table found.")
        return None

    def read_rds_table(self, db_connector, table_name):
        """
        Reads an RDS table into a Pandas DataFrame.
        """
        engine = db_connector.init_db_engine(db_connector.read_db_creds())

        if engine is None:
            print("❌ Database connection failed. Cannot read table.")
            return None

        try:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, engine)
            print(f"✅ Successfully extracted table: {table_name}")
            return df
        except Exception as e:
            print(f"❌ Error reading table {table_name}: {e}")
            return None
        
    def retrieve_pdf_data(self, link):
        pdf_path = link
        df = tb.read_pdf(pdf_path, pages="all")
        df = pd.concat(df)
        df = df.reset_index(drop=True)
        return df

    def list_number_of_stores(self, endpoint, api_key):
        response = requests.get(endpoint, headers=api_key)
        content = response.text
        result = json.loads(content)
        number_stores = result['number_stores']
        
        return number_stores

    def retrieve_stores_data(self, number_stores, endpoint, api_key):
        data = []
        for store in range(0, number_stores):
            response = requests.get(f'{endpoint}{store}', headers=api_key)
            content = response.text
            result = json.loads(content)
            data.append(result)

        df = pd.DataFrame(data)

        return df
    
    def extract_from_s3(self, s3_address):
        s3 = boto3.resource('s3')
        if 's3://' in s3_address:
            s3_address = s3_address.replace('s3://','' )
        elif 'https' in s3_address:
            s3_address = s3_address.replace('https://', '')

        bucket_name, file_key = s3_address.split('/', 1)
        bucket_name = 'data-handling-public'
        obj = s3.Object(bucket_name, file_key)
        body = obj.get()['Body']
        if 'csv' in file_key:
            df = pd.read_csv(body)
        elif '.json' in file_key:
            df = pd.read_json(body)
        df = df.reset_index(drop=True)
        return df

 

