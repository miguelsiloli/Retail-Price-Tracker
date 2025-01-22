# Decorator for retrying a function call
from functools import wraps
import requests
import time
import boto3
from botocore.client import Config
from dotenv import load_dotenv
import os
from b2sdk.v2 import *

# Load environment variables from .env file
load_dotenv()

info = InMemoryAccountInfo()
b2_api = B2Api(info)
b2_api.authorize_account(
    "production",
    os.getenv("B2_KEY_ID"),
    os.getenv("B2_PASSWORD")
)


def retry_on_failure(retries=3, delay=60):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = retries
            while attempts > 0:
                try:
                    return func(*args, **kwargs)
                except requests.RequestException as e:
                    attempts -= 1
                    print(
                        f"Request failed: {e}. Retrying in {delay} seconds...")
                    time.sleep(delay)
            raise Exception(
                f"Failed to complete {func.__name__} after {retries} retries.")

        return wrapper

    return decorator

SUPABASE_S3_CREDENTIALS = {
    "access_key_id": os.getenv("SUPABASE_ACCESS_KEY_ID"),
    "secret_access_key": os.getenv("SUPABASE_SECRET_ACCESS_KEY"),
    "endpoint": os.getenv("SUPABASE_ENDPOINT"),
    "region": os.getenv("SUPABASE_REGION"),
    "bucket_name": os.getenv("SUPABASE_BUCKET_NAME"),
}

# # S3 Client Initialization
# s3_client = boto3.client(
#     "s3",
#     aws_access_key_id=SUPABASE_S3_CREDENTIALS["access_key_id"],
#     aws_secret_access_key=SUPABASE_S3_CREDENTIALS["secret_access_key"],
#     endpoint_url=SUPABASE_S3_CREDENTIALS["endpoint"],
#     region_name=SUPABASE_S3_CREDENTIALS["region"],
#     config=Config(signature_version="s3v4"),
# )

# def upload_csv_to_supabase_s3(file_path, folder_name, logger, s3_client = s3_client):
#     """
#     Uploads a CSV file to Supabase storage within a specified folder.
#     Creates the folder if it does not exist.
#     """
#     file_name = os.path.basename(file_path)
#     remote_path = f"{folder_name}/{file_name}"

#     try:
#         s3_client.upload_file(file_path, SUPABASE_S3_CREDENTIALS["bucket_name"], remote_path)
#         logger.info(f"Uploaded '{file_name}' to Supabase at '{remote_path}'.")
#     except Exception as e:
#         logger.error(f"Failed to upload '{file_name}' to Supabase: {str(e)}", exc_info=True)


def upload_csv_to_supabase_s3(file_path, folder_name, logger):
    """
    Uploads a CSV file to Supabase storage within a specified folder.
    Creates the folder if it does not exist.
    """
    file_name = os.path.basename(file_path)
    remote_path = f"{folder_name}/{file_name}"

    bucket = b2_api.get_bucket_by_name(os.getenv("B2_BUCKET"))

    file_info = {'Content-Type': 'text/csv'}

    try:
        bucket.upload_local_file(
            local_file=file_path,
            file_name=remote_path,
            file_info=file_info
        )
        logger.info(f"Successfully uploaded '{file_name}' to Supabase at '{remote_path}'.")
        return True

    except Exception as e:
        logger.error(f"Failed to upload '{file_name}' to Supabase: {str(e)}", exc_info=True)
        return False