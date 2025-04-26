import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import os
import pandas as pd
import time
import json
from datetime import datetime
# Assuming utils.py contains upload_csv_to_gcs and logger.py is no longer needed for setup
from utils import upload_csv_to_gcs # Removed retry_on_failure, upload_csv_to_supabase_s3, setup_logger
from prefect import task, flow, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
import warnings

# Ignore specific pandas warnings if necessary (e.g., about DataFrame concatenation)
warnings.filterwarnings("ignore", category=FutureWarning, module="pandas.core.frame")


@task(
    name="Parse Products from HTML",
    # Cache results based on input HTML content hash
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1), # Optional: cache results for a day
    log_prints=True
)
def parse_products_from_html(html_content: str) -> pd.DataFrame:
    """
    Parses HTML content to extract product information and returns it as a DataFrame.

    Args:
        html_content (str): The raw HTML content of the page to be parsed.

    Returns:
        pd.DataFrame: A DataFrame containing parsed product information. Returns an empty DataFrame if no products are found or on parsing error.
    """
    logger = get_run_logger()
    soup = BeautifulSoup(html_content, 'html.parser')

    # Define the schema for the product information DataFrame
    product_schema = {
        "product_id": pd.Series(dtype='str'),
        "product_name": pd.Series(dtype='str'),
        "product_price": pd.Series(dtype='float'),
        "product_category": pd.Series(dtype='str'),
        "product_category2": pd.Series(dtype='str'),
        "product_category3": pd.Series(dtype='str'),
        "product_image": pd.Series(dtype='str'),
        "product_urls": pd.Series(dtype='str'),
        "product_ratings": pd.Series(dtype='str'),
        "product_labels": pd.Series(dtype='str'),
        "product_promotions": pd.Series(dtype='str'),
        # "quantity_selector": pd.Series(dtype='str') # Keep commented out as in original
    }

    # Create an empty DataFrame with the defined schema
    # product_df = pd.DataFrame(product_schema) # Not needed, build list first

    products = soup.find_all('div', class_='product')
    if not products:
        logger.info("No product elements found on the page.")
        return pd.DataFrame(columns=product_schema.keys()) # Return empty DataFrame matching schema

    product_list = []

    for product in products:
        try:
            product_data = {}

            # Extract product ID
            product_data['product_id'] = product['data-pid']

            # Extract product URLs
            product_urls_div = product.find('div', class_='product-tile')
            product_data['product_urls'] = product_urls_div['data-urls'] if product_urls_div else None

            # Extract product name
            product_name_link = product.find('div', class_='pdp-link')
            if product_name_link and product_name_link.find('a'):
                 product_data['product_name'] = product_name_link.find('a').text.strip()
            else:
                 product_data['product_name'] = None
                 logger.warning(f"Could not find product name for product ID: {product_data.get('product_id', 'N/A')}")


            # Extract product price
            product_price_span = product.find('span', class_='value')
            product_data['product_price'] = float(product_price_span['content']) if product_price_span else None

            # Extract product category JSON string
            product_category_str = product_urls_div['data-gtm-new'] if product_urls_div else '{}'
            
            # Extract nested product categories safely
            try:
                product_category_data = json.loads(product_category_str)
                product_data['product_category'] = product_category_data.get('item_category', None)
                product_data['product_category2'] = product_category_data.get('item_category2', None)
                product_data['product_category3'] = product_category_data.get('item_category3', None)
            except json.JSONDecodeError:
                logger.warning(f"Could not parse category JSON for product ID {product_data.get('product_id', 'N/A')}: {product_category_str}")
                product_data['product_category'] = None
                product_data['product_category2'] = None
                product_data['product_category3'] = None

            # Extract product image URL
            product_image_container = product.find('div', class_='image-container')
            product_image_tag = product_image_container.find('img') if product_image_container else None
            product_data['product_image'] = product_image_tag['src'] if product_image_tag else None

            # Extract product ratings ID
            product_ratings_div = product.find('div', class_='auc-product-tile__bazaarvoice--ratings')
            product_data['product_ratings'] = product_ratings_div['data-bv-product-id'] if product_ratings_div else None

            # Extract product labels
            product_labels_list = []
            labels = product.find_all('img', class_='auc-product-labels__icon')
            for label in labels:
                product_labels_list.append({
                    'alt': label.get('alt', ''),
                    'title': label.get('title', ''),
                    # 'src': label['src'] # Keep commented out as in original
                })
            product_data['product_labels'] = json.dumps(product_labels_list) # Store as JSON string

            # Extract product promotions (assign None if not found)
            product_promotions_div = product.find('div', class_='auc-price__promotion__label')
            product_data['product_promotions'] = product_promotions_div.text.strip() if product_promotions_div else None

            # Keep quantity selector commented out
            # quantity_selector = product.find('div', class_='auc-qty-selector')
            # product_data['quantity_selector'] = str(quantity_selector) if quantity_selector else None

            # Convert complex types explicitly to string/JSON string for DataFrame compatibility if needed (ratings already string, labels now JSON string)
            product_data["product_urls"] = str(product_data["product_urls"]) if product_data["product_urls"] is not None else None
            # product_data["product_ratings"] = str(product_data["product_ratings"]) # Already string or None
            # product_data["product_labels"] = str(product_data["product_labels"]) # Now JSON string

            product_list.append(product_data)

        except Exception as e:
            pid = product.get('data-pid', 'N/A')
            logger.error(f"Error parsing product data for product ID {pid}: {e}", exc_info=True)
            # Optionally, append partial data or skip the product
            continue # Skip this product if parsing fails

    # Convert the list of product data to a DataFrame
    if not product_list:
        logger.info("Product list is empty after parsing loop.")
        return pd.DataFrame(columns=product_schema.keys())

    product_df = pd.DataFrame(product_list)

    # Reindex the DataFrame to ensure it has the same columns as the schema, filling missing columns with NaN
    product_df = product_df.reindex(columns=product_schema.keys())

    # Basic validation (already ensured by reindex)
    # logger.debug(f"DataFrame columns: {list(product_df.columns)}")
    # logger.debug(f"Schema columns: {list(product_schema.keys())}")
    assert list(product_df.columns) == list(product_schema.keys()), "DataFrame structure does not match the schema after reindex"

    logger.info(f"Successfully parsed {len(product_df)} products from HTML.")
    return product_df


@task(
    name="Get Auchan Page Data",
    retries=3,
    retry_delay_seconds=60,
    log_prints=True
)
def get_auchan_data(cgid: str, prefn1: str, prefv1: str, start: int, sz: int, next_page: str, selectedUrl: str) -> str:
    """
    Fetches HTML data from the Auchan store's search API endpoint.

    Args:
        cgid (str): The category group ID.
        prefn1 (str): The name of the first preference filter.
        prefv1 (str): The value of the first preference filter.
        start (int): The starting index for the product list.
        sz (int): The number of products to fetch per request.
        next_page (str): Usually "true" to indicate fetching page data.
        selectedUrl (str): The URL constructed for logging/debugging purposes.

    Returns:
        str: The raw HTML content from the Auchan store's search results.

    Raises:
        requests.exceptions.RequestException: If the request fails after retries.
    """
    logger = get_run_logger()
    url = "https://www.auchan.pt/on/demandware.store/Sites-AuchanPT-Site/pt_PT/Search-UpdateGrid"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "TE": "Trailers"
    }
    params = {
        "cgid": cgid,
        "prefn1": prefn1,
        "prefv1": prefv1,
        "start": start,
        "sz": sz,
        "next": next_page # Renamed argument for clarity
    }

    logger.info(f"Attempting to GET data from Auchan API. URL (constructed): {selectedUrl}")
    # logger.debug(f"Request params: {params}")
    response = requests.get(url, headers=headers, params=params, timeout=30) # Added timeout

    if response.status_code == 200:
        logger.info(f"Successfully fetched data for start={start}, sz={sz}. Status: {response.status_code}")
        return response.text
    else:
        logger.error(f"Failed to fetch data for start={start}, sz={sz}. Status: {response.status_code}, Response: {response.text[:200]}...") # Log part of response
        response.raise_for_status() # Raise HTTPError for non-200 status codes


@task(
    name="Get and Parse All Auchan Data for CGID",
    log_prints=True
)
def get_and_parse_auchan_data_for_cgid(cgid: str, prefn1: str, prefv1: str, sz: int, base_url: str) -> pd.DataFrame:
    """
    Retrieves and parses all product data for a specific Auchan cgid using pagination.

    Args:
        cgid (str): The category group ID.
        prefn1 (str): The name of the first preference filter.
        prefv1 (str): The value of the first preference filter.
        sz (int): The number of products to fetch per page request.
        base_url (str): The base URL of the search results (used for logging context).

    Returns:
        pd.DataFrame: A DataFrame containing all parsed product information for the cgid.
    """
    logger = get_run_logger()
    start = 0
    all_data_list = []
    max_pages = 100 # Safety break to prevent infinite loops
    page_count = 0

    logger.info(f"Starting paginated fetch for cgid: {cgid}")

    # Using tqdm for progress within the task, consider removing if running many tasks in parallel
    # with tqdm(total=None, unit='page', desc=f"Fetching cgid {cgid}") as pbar:
    while page_count < max_pages:
        page_count += 1
        selectedUrl = f"{base_url}?cgid={cgid}&prefn1={prefn1}&prefv1={prefv1}&start={start}&sz={sz}&next=true"
        logger.info(f"Fetching page {page_count} (start={start}, sz={sz}) for cgid {cgid}")

        try:
            # Call the task to get HTML data
            html_data = get_auchan_data(
                cgid=cgid,
                prefn1=prefn1,
                prefv1=prefv1,
                start=start,
                sz=sz,
                next_page="true",
                selectedUrl=selectedUrl
            )
            # Check if returned html_data is empty or indicates no more products
            # (Auchan might return minimal HTML or a specific message when empty)
            if not html_data or 'class="product"' not in html_data:
                 logger.info(f"No more product data found or empty HTML received for cgid {cgid} at start={start}. Stopping pagination.")
                 break

            # Call the task to parse HTML
            parsed_data_df = parse_products_from_html(html_data)

            if parsed_data_df.empty:
                logger.info(f"Parsing returned empty DataFrame for cgid {cgid} at start={start}. Assuming end of results.")
                break

            all_data_list.append(parsed_data_df)
            num_parsed = len(parsed_data_df)
            logger.info(f"Parsed {num_parsed} products from page {page_count} for cgid {cgid}.")

            # pbar.update(1) # Update tqdm progress bar

            # If fewer products were returned than requested, it's likely the last page
            if num_parsed < sz:
                logger.info(f"Received {num_parsed} products (less than requested sz={sz}). Assuming last page for cgid {cgid}.")
                break

            start += sz # Increment start for the next page
            time.sleep(3) # Respectful delay between requests

        except Exception as e:
            logger.error(f"Error during pagination for cgid {cgid} at start={start}: {e}", exc_info=True)
            # Decide whether to break or continue (e.g., after retries failed in get_auchan_data)
            break # Stop processing this cgid on error

    if not all_data_list:
        logger.warning(f"No data collected for cgid: {cgid}. Returning empty DataFrame.")
        # Define schema for empty DataFrame if needed, though parse_products_from_html should handle schema
        return pd.DataFrame(columns=parse_products_from_html("").columns) # Get columns from schema defined in parse task

    logger.info(f"Concatenating data for cgid: {cgid} from {len(all_data_list)} pages.")
    final_df = pd.concat(all_data_list, ignore_index=True)
    logger.info(f"Finished fetching for cgid {cgid}. Total products found: {len(final_df)}")
    return final_df


@task(
    name="Process and Upload Data for One CGID",
    log_prints=True
)
def process_and_upload_one_cgid(
    cgid: str,
    prefn1: str,
    prefv1: str,
    sz: int,
    base_url: str,
    data_directory: str,
    supabase_folder: str, # Keeping supabase naming convention for folder, but uploading to GCS
    timestamp: str,
    gcs_bucket_name: str,
    credentials_dict: dict
):
    """
    Fetches, parses, processes, saves, and uploads data for a single cgid.
    """
    logger = get_run_logger()
    logger.info(f"Starting processing for cgid: {cgid}")

    try:
        # Fetch and parse data for this cgid
        final_data = get_and_parse_auchan_data_for_cgid(
            cgid=cgid,
            prefn1=prefn1,
            prefv1=prefv1,
            sz=sz,
            base_url=base_url
        )

        if not final_data.empty:
            # Add metadata columns
            final_data["source"] = "auchan"
            final_data["timestamp"] = timestamp # Use the flow-level timestamp
            final_data["cgid"] = cgid # Add cgid for easier identification in combined data

            # Create filename and path
            filename = f"auchan_{cgid}_{timestamp}.csv"
            file_path = os.path.join(data_directory, filename)

            # Save the data locally to CSV
            final_data.to_csv(file_path, index=False, encoding='utf-8')
            logger.info(f"Data for {cgid} saved locally to {file_path}")

            # Upload to GCS
            # Assuming upload_csv_to_gcs takes logger, file_path, folder_name, bucket_name, credentials
            upload_csv_to_gcs(
                logger=logger,
                file_path=file_path,
                folder_name=supabase_folder, # Use the structured folder name
                gcs_bucket_name=gcs_bucket_name,
                credentials_dict=credentials_dict
            )
            # Optionally remove local file after upload if desired
            # os.remove(file_path)
            # logger.info(f"Removed local file: {file_path}")

        else:
            logger.warning(f"No data found or processed for {cgid}. Skipping file save and upload.")

    except Exception as e:
        logger.error(f"Failed to process cgid {cgid}: {e}", exc_info=True)
        # Depending on requirements, this task failure might stop the flow or allow others to continue


@flow(name="Auchan Product Scraper Flow", log_prints=True)
def auchan_scraper_flow(
    cgid_list: list[str],
    prefn1: str = "productAge", # Default values can be set
    prefv1: str = "none",
    sz: int = 30,
    base_url: str = "https://www.auchan.pt/pt/produtos-auchan",
    base_path: str = "data/raw",
    gcs_bucket_env_var: str = "GCS_BUCKET_NAME", # Env var name for bucket
    # Add env var names for credentials
    gcs_type_env_var: str = "TYPE",
    gcs_project_id_env_var: str = "PROJECT_ID",
    gcs_private_key_id_env_var: str = "PRIVATE_KEY_ID",
    gcs_private_key_env_var: str = "PRIVATE_KEY",
    gcs_client_email_env_var: str = "CLIENT_EMAIL",
    gcs_client_id_env_var: str = "CLIENT_ID",
    gcs_auth_uri_env_var: str = "AUTH_URI",
    gcs_token_uri_env_var: str = "TOKEN_URI",
    gcs_auth_provider_cert_url_env_var: str = "AUTH_PROVIDER_X509_CERT_URL",
    gcs_client_cert_url_env_var: str = "CLIENT_X509_CERT_URL",
    gcs_universe_domain_env_var: str = "UNIVERSE_DOMAIN"
):
    """
    Prefect flow to fetch, process, and upload Auchan product data for multiple category IDs (cgids).
    Data is saved locally and uploaded to Google Cloud Storage.
    """
    logger = get_run_logger()
    logger.info(f"Starting Auchan scraper flow for {len(cgid_list)} cgids.")

    # --- Configuration and Setup ---
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S") # More precise timestamp
    gcs_bucket_name = os.getenv(gcs_bucket_env_var)

    # Read GCS credentials from environment variables
    try:
        private_key = os.getenv(gcs_private_key_env_var)
        if private_key:
            private_key = private_key.replace("\\n", "\n")

        credentials_dict = {
            "type": os.getenv(gcs_type_env_var),
            "project_id": os.getenv(gcs_project_id_env_var),
            "private_key_id": os.getenv(gcs_private_key_id_env_var),
            "private_key": private_key,
            "client_email": os.getenv(gcs_client_email_env_var),
            "client_id": os.getenv(gcs_client_id_env_var),
            "auth_uri": os.getenv(gcs_auth_uri_env_var),
            "token_uri": os.getenv(gcs_token_uri_env_var),
            "auth_provider_x509_cert_url": os.getenv(gcs_auth_provider_cert_url_env_var),
            "client_x509_cert_url": os.getenv(gcs_client_cert_url_env_var),
            "universe_domain": os.getenv(gcs_universe_domain_env_var)
        }
        # Basic validation
        if not all([gcs_bucket_name, credentials_dict["type"], credentials_dict["project_id"], credentials_dict["private_key"], credentials_dict["client_email"]]):
             raise ValueError("Required GCS configuration (Bucket Name, Credentials) is missing in environment variables.")
        logger.info("Successfully loaded GCS configuration from environment variables.")

    except Exception as e:
        logger.error(f"Error loading GCS configuration: {e}", exc_info=True)
        # Fail the flow if GCS config is invalid
        raise ValueError("GCS configuration error") from e


    # Ensure the base local path exists
    # Use timestamp directly in the base_path for unique run folders
    data_directory = os.path.join(base_path, timestamp)
    try:
        os.makedirs(data_directory, exist_ok=True)
        logger.info(f"Local data directory ensured: '{data_directory}'")
    except OSError as e:
        logger.error(f"Could not create data directory {data_directory}: {e}", exc_info=True)
        raise # Stop flow if local dir can't be created


    # Define GCS folder structure (using supabase naming convention as requested)
    # Use a timestamp without time for the folder name if preferred for daily structure
    daily_timestamp = datetime.now().strftime("%Y%m%d")
    gcs_target_folder = f"retail_data/auchan/{daily_timestamp}"
    logger.info(f"Data will be uploaded to GCS bucket '{gcs_bucket_name}' in folder '{gcs_target_folder}'")

    # --- Execute Tasks ---
    # Use Prefect's map to run the processing task for each cgid
    # This allows for potential parallelism depending on the configured task runner
    futures = process_and_upload_one_cgid.map(
        cgid=cgid_list,
        # Pass other arguments that are constant for all mapped tasks
        prefn1=[prefn1] * len(cgid_list),
        prefv1=[prefv1] * len(cgid_list),
        sz=[sz] * len(cgid_list),
        base_url=[base_url] * len(cgid_list),
        data_directory=[data_directory] * len(cgid_list),
        supabase_folder=[gcs_target_folder] * len(cgid_list),
        timestamp=[timestamp] * len(cgid_list),
        gcs_bucket_name=[gcs_bucket_name] * len(cgid_list),
        credentials_dict=[credentials_dict] * len(cgid_list)
    )

    # Wait for all mapped tasks to complete (optional, map implicitly waits)
    # results = [f.result() for f in futures] # Can collect results if needed

    logger.info("Auchan scraper flow finished.")

