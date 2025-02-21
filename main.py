from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

# Initialize DatabaseConnector and DataExtractor
db_connector = DatabaseConnector()
extractor = DataExtractor()
cleaner = DataCleaning()  # Initialize DataCleaning class

# Step 1: Find the user table
user_table = extractor.find_user_table(db_connector)

# Step 2: Read the user data if the table is found
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


from sqlalchemy import create_engine
import pandas as pd

# Replace with your actual PostgreSQL credentials
db_creds = "postgresql://postgres:@localhost/sales_data"

# Example of how to upload cleaned data (assuming 'clean_user_data' is a pandas DataFrame)
engine = create_engine(db_creds)
clean_user_data.to_sql('dim_users', engine, if_exists='replace', index=False)

