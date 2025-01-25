
from dotenv import load_dotenv
import boto3
from botocore.client import Config
from datetime import datetime
import os
from io import StringIO
import pandas as pd
from tqdm import tqdm
from b2sdk.v2 import B2Api, InMemoryAccountInfo
import requests

load_dotenv()

SUPABASE_S3_CREDENTIALS = {
    "access_key_id": os.getenv("SUPABASE_ACCESS_KEY_ID"),
    "secret_access_key": os.getenv("SUPABASE_SECRET_ACCESS_KEY"),
    "endpoint": os.getenv("SUPABASE_ENDPOINT"),
    "region": os.getenv("SUPABASE_REGION"),
    "bucket_name": os.getenv("SUPABASE_BUCKET_NAME"),
}

# S3 Client Initialization
s3_client = boto3.client(
    "s3",
    aws_access_key_id=SUPABASE_S3_CREDENTIALS["access_key_id"],
    aws_secret_access_key=SUPABASE_S3_CREDENTIALS["secret_access_key"],
    endpoint_url=SUPABASE_S3_CREDENTIALS["endpoint"],
    region_name=SUPABASE_S3_CREDENTIALS["region"],
    config=Config(signature_version="s3v4"),
)

def concat_csv_from_supabase(folder_name, s3_client = s3_client):
    # Get current date in the required format
    
    current_date = datetime.now().strftime("%Y%m%d")

    # Specify the bucket and path
    bucket_name = "retail"
    path_prefix = f"raw/{folder_name}/"

    # List all objects in the specified path
    objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=path_prefix)

    # Initialize an empty list to store dataframes
    dfs = []

    # Read each CSV file and append to the list
    for obj in tqdm(objects.get('Contents', []), desc="Processing CSV from S3 buckets", unit="file"):
        if obj['Key'].endswith('.csv'):
            response = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
            csv_content = response['Body'].read().decode('utf-8')
            df = pd.read_csv(StringIO(csv_content))
            dfs.append(df)

    # Concatenate all dataframes
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        return combined_df
    else:
        raise Exception("No CSV files found")  # Return an empty dataframe if no CSV files found

def concat_csv_from_b2(folder_name):
    # Load B2 credentials from .env
    load_dotenv()
    B2_KEY_ID = os.getenv('B2_KEY_ID')
    B2_PASSWORD = os.getenv('B2_PASSWORD')
    B2_BUCKET = os.getenv('B2_BUCKET')
    
    # First, authorize with B2
    auth_response = requests.get(
        'https://api.backblazeb2.com/b2api/v3/b2_authorize_account',
        auth=(B2_KEY_ID, B2_PASSWORD)
    )
    auth_data = auth_response.json()
    
    # Get the authorization token and API URL
    auth_token = auth_data['authorizationToken']
    api_url = auth_data['apiInfo']['storageApi']['apiUrl']
    
    headers = {
        'Authorization': auth_token,
    }
    
    list_buckets_url = f"{api_url}/b2api/v3/b2_list_buckets"
    bucket_params = {
        'accountId': auth_data['accountId']
    }
    buckets_response = requests.get(list_buckets_url, headers=headers, params=bucket_params)
    buckets_data = buckets_response.json()

    retail_bucket = next((b for b in buckets_data['buckets'] if b['bucketName'] == B2_BUCKET), None)
    if not retail_bucket:
        raise Exception("Retail bucket not found")

    # List files
    dfs = []
    start_file_name = None
    path_prefix = f"raw/{folder_name}/"
    
    while True:
        list_files_url = f"{api_url}/b2api/v3/b2_list_file_names"
        params = {
            'bucketId': retail_bucket["bucketId"],
            'prefix': path_prefix,
            'startFileName': start_file_name,
            'maxFileCount': 1000
        }
        
        response = requests.get(list_files_url, headers=headers, params=params)
        files_data = response.json()
        
        for file_info in tqdm(files_data['files'], desc="Processing files"):
            if file_info['fileName'].endswith('.csv'):
                # Get download URL for the file
                download_url = f"{api_url}/b2api/v3/b2_download_file_by_id"
                file_response = requests.get(
                    download_url,
                    headers=headers,
                    params={'fileId': file_info['fileId']}
                )
                
                # Read the CSV content
                csv_content = StringIO(file_response.text)
                df = pd.read_csv(csv_content)
                dfs.append(df)
                print(f"Successfully processed: {file_info['fileName']}")
        
        # Check if there are more files
        if not files_data.get('nextFileName'):
            break
        start_file_name = files_data['nextFileName']
    
    # Concatenate all dataframes
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        return combined_df
    else:
        raise Exception("No CSV files found")


df = concat_csv_from_b2('continente/20250125')
print(df)
