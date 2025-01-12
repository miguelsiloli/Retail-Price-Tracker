
from dotenv import load_dotenv
import boto3
from botocore.client import Config
from datetime import datetime
import os
from io import StringIO
import pandas as pd
from tqdm import tqdm

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