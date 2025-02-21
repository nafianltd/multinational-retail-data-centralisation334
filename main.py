from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from sqlalchemy import create_engine
import pandas as pd

db_connector = DatabaseConnector()
extractor = DataExtractor()
cleaner = DataCleaning()  # Initialize DataCleaning class
db_creds = "postgresql://postgres:Uzair123@localhost/sales_data"
engine = create_engine(db_creds)

# User data 
user_table = extractor.find_user_table(db_connector)
if user_table:
    user_data = extractor.read_rds_table(db_connector, user_table)
    
    if user_data is not None:
        print("✅ User data extracted successfully.")
        
        # Step 3: Clean the user data
        clean_user_data = cleaner.clean_user_data(user_data)
        print("✅ User data cleaned successfully.")

        # Step 4: Upload the cleaned data to 'dim_users' in sales_data
        db_creds = db_connector.read_db_creds()  # Read credentials
        db_connector.upload_to_db(clean_user_data, "dim_users", db_creds)
        print("🚀 User data uploaded successfully to 'dim_users' in 'sales_data' database.")
    else:
        print("❌ Failed to extract user data.")
else:
    print("❌ No user table found.")


# Card data
pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
card_data = extractor.retrieve_pdf_data(pdf_link)

if card_data is not None:
    print("✅ Card data extracted successfully from PDF.")

    # Step 3: Clean the card data
    clean_card_data = cleaner.clean_card_data(card_data)
    print("✅ Card data cleaned successfully.")

    # Step 4: Upload the cleaned card data to 'dim_card_details' in sales_data
    db_creds = db_connector.read_db_creds()  # Read credentials
    db_connector.upload_to_db(clean_card_data, "dim_card_details", db_creds)
    print("🚀 Card data uploaded successfully to 'dim_card_details' in 'sales_data' database.")
else:
    print("❌ Failed to extract card data from the PDF.")

# Stores Data
api_key = "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
number_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
store_details_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"

number_of_stores = extractor.list_number_of_stores(number_of_stores_endpoint, api_key)
print(f"✅ Number of stores: {number_of_stores}")
store_data = extractor.retrieve_stores_data(number_of_stores, store_details_endpoint, api_key)
print(f"✅ Retrieved store data. Number of records: {len(store_data)}")

cleaned_store_data = cleaner.clean_store_data(store_data)
print(f"✅ Cleaned store data. Number of records: {len(cleaned_store_data)}")

db_creds = db_connector.read_db_creds()
db_connector.upload_to_db(cleaned_store_data, "dim_store_details", db_creds)
print("✅ Store data uploaded to the 'dim_store_details' table successfully.")


# Product Data
product_data = extractor.extract_from_s3('s3://data-handling-public/products.csv')
cleaned_product_data = cleaner.clean_product_data(product_data)
cleaned_product_data.to_csv('product.csv')
db_connector.upload_to_db(cleaned_product_data, 'dim_products', db_creds)

# Orders Data
orders_table = extractor.find_orders_table(db_connector)
if orders_table:
    orders_data = extractor.read_rds_table(db_connector, orders_table)
    
    if orders_data is not None:
        print("✅ Order data extracted successfully.")
        
        # Step 3: Clean the user data
        clean_orders_data = cleaner.clean_order_data(orders_data)
        print("✅ Order data cleaned successfully.")

        # Step 4: Upload the cleaned data to 'dim_users' in sales_data
        db_creds = db_connector.read_db_creds()  # Read credentials
        db_connector.upload_to_db(clean_orders_data, "orders_table", db_creds)
        print("🚀 Orders data uploaded successfully to 'orders_table' in 'sales_data' database.")
    else:
        print("❌ Failed to extract orders data.")
else:
    print("❌ No orders table found.")

# Date Events Data
date_data = extractor.extract_from_s3('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
clean_date_data = cleaner.clean_date_data(date_data)
clean_date_data.to_csv('date.csv')
db_connector.upload_to_db(clean_date_data, "dim_date_times", db_creds)