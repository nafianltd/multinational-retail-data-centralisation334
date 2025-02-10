import pandas as pd
import requests
import boto3
import io

class DataExtractor:
    """A utility class to extract data from various sources."""

    def extract_from_csv(self, file_path):
        """Extracts data from a CSV file and returns a DataFrame."""
        return pd.read_csv(file_path)

    def extract_from_api(self, api_url, headers=None):
        """Extracts data from an API and returns a DataFrame."""
        response = requests.get(api_url, headers=headers)
        if response.status_code == 200:
            return pd.DataFrame(response.json())  # Assumes the response is a list of dictionaries
        else:
            raise Exception(f"API request failed with status code {response.status_code}")

    def extract_from_s3(self, bucket_name, file_key, aws_access_key, aws_secret_key):
        """Extracts data from an S3 bucket and returns a DataFrame."""
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )
        obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        return pd.read_csv(io.BytesIO(obj['Body'].read()))
