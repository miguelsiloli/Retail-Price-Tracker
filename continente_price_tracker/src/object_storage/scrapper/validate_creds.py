import os
import sys
from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account
from google.api_core import exceptions

# --- Load Environment Variables ---
load_dotenv()

def load_and_check_env_vars():
    """Loads and validates the presence of required environment variables."""
    print("1. Loading credentials from environment variables...")
    
    gcs_bucket_name = os.getenv("GCS_BUCKET_NAME")
    project_id = os.getenv("PROJECT_ID")
    private_key = os.getenv("PRIVATE_KEY")
    client_email = os.getenv("CLIENT_EMAIL")
    
    # --- Check for missing variables ---
    missing_vars = []
    if not gcs_bucket_name: missing_vars.append("GCS_BUCKET_NAME")
    if not project_id: missing_vars.append("PROJECT_ID")
    if not private_key: missing_vars.append("PRIVATE_KEY")
    if not client_email: missing_vars.append("CLIENT_EMAIL")
    
    if missing_vars:
        print(f"\n❌ VALIDATION FAILED: The following required environment variables are missing:")
        for var in missing_vars:
            print(f"  - {var}")
        print("\nPlease check your .env file or environment configuration.")
        return None, None

    # Replace literal '\n' with actual newlines for the private key
    private_key = private_key.replace('\\n', '\n')
    
    credentials_dict = {
        "type": "service_account",
        "project_id": project_id,
        "private_key_id": os.getenv("PRIVATE_KEY_ID"),
        "private_key": private_key,
        "client_email": client_email,
        "client_id": os.getenv("CLIENT_ID"),
        "auth_uri": os.getenv("AUTH_URI", "https://accounts.google.com/o/oauth2/auth"),
        "token_uri": os.getenv("TOKEN_URI", "https://oauth2.googleapis.com/token"),
        "auth_provider_x509_cert_url": os.getenv("AUTH_PROVIDER_X509_CERT_URL", "https://www.googleapis.com/oauth2/v1/certs"),
        "client_x509_cert_url": os.getenv("CLIENT_X509_CERT_URL")
    }
    
    print("   - Successfully loaded variables from .env file.")
    return credentials_dict, gcs_bucket_name

def validate_gcs_access(credentials_dict, bucket_name):
    """
    Attempts to connect to GCS and retrieve metadata about the specified bucket.
    """
    try:
        print("\n2. Attempting to authenticate with Google Cloud...")
        
        # Create credentials object from the dictionary
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        
        # Create the GCS client with the explicit credentials
        storage_client = storage.Client(credentials=credentials, project=credentials_dict['project_id'])
        
        print("   - Authentication successful. Service Account Email:", credentials.service_account_email)
        
        print(f"\n3. Verifying access to bucket '{bucket_name}' (read-only)...")
        
        # The core validation step: try to get the bucket.
        # This is a read-only API call that confirms the bucket exists and we have permission.
        bucket = storage_client.get_bucket(bucket_name)
        
        print("\n" + "="*50)
        print("✅ SUCCESS: Credentials are valid and have access!")
        print("="*50)
        print(f"   - Bucket Name: {bucket.name}")
        print(f"   - Bucket Location: {bucket.location}")
        print(f"   - Bucket Storage Class: {bucket.storage_class}")
        return True

    except exceptions.Forbidden as e:
        print("\n❌ VALIDATION FAILED: Permission Denied.")
        print("   - The credentials are VALID, but they do NOT have permission to access this bucket.")
        print("   - Action Required: Go to the GCP IAM page and ensure the service account")
        print(f"     '{credentials.service_account_email}' has a role like 'Storage Object Viewer' or 'Storage Object Admin' on the bucket '{bucket_name}'.")
        # print(f"   - Full error: {e}")
        return False
        
    except exceptions.NotFound as e:
        print(f"\n❌ VALIDATION FAILED: Bucket Not Found.")
        print(f"   - The bucket named '{bucket_name}' does not seem to exist in the project '{credentials_dict['project_id']}'.")
        print("   - Action Required: Please verify the GCS_BUCKET_NAME in your .env file is correct.")
        return False
        
    except Exception as e:
        print(f"\n❌ VALIDATION FAILED: An unexpected error occurred.")
        print("   - This could be due to an invalid private key, a disabled service account, or a network issue.")
        print(f"   - Action Required: Double-check all credential values in your .env file. Ensure the service account is enabled in GCP.")
        print(f"   - Full error: {e}")
        return False

if __name__ == "__main__":
    creds, bucket = load_and_check_env_vars()
    
    if creds and bucket:
        validate_gcs_access(creds, bucket)