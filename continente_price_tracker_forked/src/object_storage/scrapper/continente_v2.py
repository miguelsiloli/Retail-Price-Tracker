import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import re
import time
import random
from datetime import datetime, timedelta
import os
import logging
from functools import wraps
from typing import List, Dict, Any, Optional, Generator, Tuple
from tqdm import tqdm
# from utils import retry_on_failure, upload_parquet_to_supabase_s3 # Assuming retry decorator is here

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
# b2_api.authorize_account(
#     "production",
#     os.getenv("B2_KEY_ID"),
#     os.getenv("B2_PASSWORD")
# )

# --- Configuration ---
BASE_URL_CONTINENTE = "https://www.continente.pt/on/demandware.store/Sites-continente-Site/default/Search-UpdateGrid"
DEFAULT_SZ_CONTINENTE = 96  # Default page size (adjust based on observation)
DEFAULT_PMIN_CONTINENTE = "0.01" # Default minimum price filter
DEFAULT_HEADERS_CONTINENTE = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br", # Removed zstd for broader compatibility, add back if needed
    "Accept-Language": "en-US,en;q=0.5", # Changed language preference
    "Connection": "keep-alive",
    "Host": "www.continente.pt",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36", # Standard UA
    "Upgrade-Insecure-Requests": "1",
    "TE": "trailers"
}
RETRY_CONFIG_CONTINENTE = {"retries": 3, "delay": 120} # Delay in seconds
POLITENESS_DELAY_CONTINENTE = (3, 5) # Range for random delay between requests (seconds)

# Output Directories
OUTPUT_DIR_CONTINENTE = "data/processed/continente"
LOG_DIR_CONTINENTE = "logs"
METRICS_DIR_CONTINENTE = "data/metrics/continente"

# List of categories (cgid values) to fetch
CATEGORIES_CONTINENTE = [
    "congelados", "frescos", "mercearia", "bebidas", "biologicos-e-escolhas-alimentares", # Adjusted based on typical URLs
    "limpeza-do-lar-e-roupa", "higiene-e-beleza", "bebe" # Adjusted based on typical URLs
    # Add/verify actual cgid values from Continente website URLs
]


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

# --- Utility Functions --- (setup_logger assumed to be defined as in Auchan script)
def setup_logger(log_directory: str, filename_prefix: str = "continente_pipeline") -> logging.Logger:
    """Sets up a logger that writes to both console and file."""
    os.makedirs(log_directory, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"{filename_prefix}_{timestamp}.log"
    log_path = os.path.join(log_directory, log_filename)

    logger = logging.getLogger(filename_prefix)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        # File Handler
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(module)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.INFO)
        logger.addHandler(file_handler)
        # Console Handler
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(logging.INFO)
        logger.addHandler(console_handler)

    return logger

# --- Stage 1: Task Generation ---
def generate_category_tasks(category_list: List[str]) -> List[Dict[str, str]]:
    """ Generates a list of task dictionaries, one for each category ID (cgid). """
    return [{"cgid": category} for category in category_list]

# --- Stage 2: HTML Fetching ---

# Use the imported retry_on_failure decorator
@retry_on_failure(retries=RETRY_CONFIG_CONTINENTE["retries"], delay=RETRY_CONFIG_CONTINENTE["delay"])
def fetch_single_page_html(
    url: str,
    params: Dict[str, Any],
    headers: Dict[str, str],
    logger: logging.Logger
) -> str:
    """ Fetches HTML content for a single page request using the retry decorator. """
    try:
        # logger.debug(f"Fetching URL: {url} with params: {params}")
        response = requests.get(url, params=params, headers=headers, timeout=45) # Increased timeout
        response.raise_for_status() # Raise HTTPError for bad responses (4XX, 5XX)
        logger.debug(f"Successfully fetched {response.url}")
        # Increment API call count in the caller *after* success
        return response.text
    except requests.exceptions.Timeout:
        logger.warning(f"Timeout occurred fetching {url} with params {params}")
        raise # Re-raise for retry decorator
    except requests.exceptions.RequestException as e:
        logger.warning(f"Request failed for {url} with params {params}: {e}")
        raise e # Re-raise for retry decorator
    except Exception as e:
        logger.error(f"Non-request exception during fetch for {url}: {e}", exc_info=True)
        raise e


def fetch_all_html_for_category(
    category_task: Dict[str, str],
    base_url: str,
    sz: int,
    pmin: str,
    headers: Dict[str, str],
    logger: logging.Logger,
    metrics_collector: Dict[str, Any]
) -> Generator[Tuple[str, int], None, Optional[int]]:
    """
    Fetches HTML content for pages of a category, yielding page content and start index.

    Args:
        category_task: Dictionary containing 'cgid'.
        base_url: The base URL for the search endpoint.
        sz: The number of products per page (page size).
        pmin: Minimum price filter value.
        headers: Dictionary of request headers.
        logger: The logger instance.
        metrics_collector: Dictionary to update with API call count.

    Yields:
        Tuple[str, int]: HTML content (str) and start index (int).

    Returns:
        Optional[int]: The total number of products found for the category,
                       extracted from the first page, or None if extraction fails.
                       Returning this allows the caller to manage the loop.
    """
    start = 0
    total_products = None
    cgid = category_task['cgid']

    logger.info(f"Starting fetch for category: {cgid}, page size: {sz}")

    while True: # Loop managed by caller based on total_products
        params = {
            "cgid": cgid,
            "pmin": pmin,
            "start": start,
            "sz": sz,
            # "srule": srule, # Removed srule unless needed and passed in
        }
        logger.info(f"Fetching page for {cgid}, start index: {start}")

        try:
            # Pass logger for potential internal use or detailed post-retry logging
            html_content = fetch_single_page_html(base_url, params=params, headers=headers, logger=logger)

            # --- METRICS: Increment API call count AFTER successful fetch ---
            metrics_collector["total_num_api_calls"] = metrics_collector.get("total_num_api_calls", 0) + 1
            # --- End Metrics ---

            # Parse total products ONLY on the first successful fetch
            if start == 0:
                total_products = parse_total_products(html_content, logger) # Pass logger
                if total_products is None:
                    logger.warning(f"Could not parse total products for {cgid} from first page.")
                    # Yield content anyway, let caller decide how to proceed
                else:
                     logger.info(f"Parsed total products for {cgid}: {total_products}")

            yield html_content, start

            # Check if we've likely fetched all products (caller handles actual stop)
            if total_products is not None and (start + sz >= total_products):
                 logger.debug(f"Fetch loop for {cgid} reached expected end based on total_products.")
                 break # Stop the *generator* loop

            start += sz
            # Random delay between requests
            delay = random.randint(POLITENESS_DELAY_CONTINENTE[0], POLITENESS_DELAY_CONTINENTE[1])
            logger.debug(f"Waiting for {delay} seconds before next request for {cgid}")
            time.sleep(delay)

        except requests.exceptions.RequestException as e:
            logger.error(f"Permanent error fetching page for {cgid} at start={start} (after retries): {e}", exc_info=False)
            # Stop generator if a page fails permanently
            break
        except Exception as e:
             logger.error(f"Unexpected error during fetch loop for {cgid} at start={start}: {e}", exc_info=True)
             break

    # Return the total count parsed from the first page
    return total_products


# --- Stage 3: HTML Parsing ---

def parse_total_products(html_content: str, logger: logging.Logger) -> Optional[int]:
    """ Parses HTML to find the total number of products displayed. """
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        # Updated class based on common patterns, verify with actual Continente HTML
        counter_div = soup.find("div", class_="search-results-products-counter")
        if not counter_div: # Fallback to the original class if needed
             counter_div = soup.find("div", class_="search-results-products-counter d-flex justify-content-center")

        if counter_div and counter_div.text:
            numbers = re.findall(r'\d+', counter_div.text.replace('.', '')) # Handle thousands separators like '1.234'
            if numbers:
                # Often shows "1-72 de 1234". We want the last number.
                total = int(numbers[-1])
                logger.debug(f"Extracted total products count: {total}")
                return total
            else:
                 logger.warning(f"Regex found no numbers in counter text: {counter_div.text}")
        else:
            logger.warning("Could not find total products counter div or div had no text.")

    except Exception as e:
        logger.error(f"Error parsing total products: {e}", exc_info=True)
    return None


def _parse_product_json(json_string: str, pid_for_log: str, logger: logging.Logger) -> Optional[Dict[str, Any]]:
    """ Safely parses the product JSON string embedded in the tile. """
    if not json_string:
        return None
    try:
        # Attempt direct parsing first
        return json.loads(json_string)
    except json.JSONDecodeError as e1:
        # If direct parsing fails, try the replace hack (with caution and logging)
        logger.warning(f"Direct JSON parsing failed for PID {pid_for_log}: {e1}. Attempting replace hack...")
        try:
            # This replace is risky - it might corrupt valid JSON containing intended single quotes.
            # Only use if absolutely necessary and confirmed by inspection.
            corrected_json_string = json_string.replace("'", '"') # Try replacing single quotes with double
            # Alternative, more complex: Use regex for smarter replacement if needed
            return json.loads(corrected_json_string)
        except json.JSONDecodeError as e2:
            logger.error(f"JSON parsing failed even after replace hack for PID {pid_for_log}: {e2}. Original JSON: {json_string[:500]}...")
            return None


def parse_products_from_html(html_content: str, logger: logging.Logger) -> List[Dict[str, Any]]:
    """ Parses HTML content to extract product information. """
    products_data = []
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        products = soup.find_all('div', class_='product')

        if not products:
            logger.debug("No product elements found on the page.")
            return []

        for product in products:
            product_data = {}
            try:
                product_data['product_id'] = product.get('data-pid')
                if not product_data['product_id']:
                     logger.warning("Found product div with no data-pid. Skipping.")
                     continue # Skip if essential ID is missing

                tile_div = product.find('div', class_='product-tile')
                product_data['product_urls'] = tile_div.get('data-urls') if tile_div else None

                name_link = product.find('div', class_='pdp-link')
                product_data['product_name'] = name_link.find('a').text.strip() if name_link and name_link.find('a') else None

                price_span = product.find('span', class_='value')
                if price_span and price_span.get('content'):
                    try: product_data['product_price'] = float(price_span['content'])
                    except (ValueError, TypeError): product_data['product_price'] = None
                else: product_data['product_price'] = None

                category_str = tile_div.get('data-gtm-new') if tile_div else None
                product_data['product_category_raw'] = category_str
                product_data['product_category'], product_data['product_category2'], product_data['product_category3'] = None, None, None
                if category_str:
                    try:
                        # First attempt: direct JSON parsing
                        category_data = json.loads(category_str)
                        product_data['product_category'] = category_data.get('item_category')
                        product_data['product_category2'] = category_data.get('item_category2')
                        product_data['product_category3'] = category_data.get('item_category3')
                    except json.JSONDecodeError as e:
                        logger.warning(f"Direct JSON parsing failed for PID {product_data.get('product_id', 'N/A')}: {str(e)}. Attempting fixes...")
                        try:
                            # Second attempt: Replace problematic characters
                            fixed_str = category_str.replace("'", "\\'").replace('\\"', '\\\\"')
                            # Try to use ast.literal_eval which is more forgiving
                            import ast
                            category_data = ast.literal_eval(fixed_str)
                            product_data['product_category'] = category_data.get('item_category')
                            product_data['product_category2'] = category_data.get('item_category2')
                            product_data['product_category3'] = category_data.get('item_category3')
                            logger.info(f"Successfully parsed JSON with fixes for PID {product_data.get('product_id', 'N/A')}")
                        except Exception as e2:
                            logger.error(f"All JSON parsing attempts failed for PID {product_data.get('product_id', 'N/A')}: {str(e2)}")

                img_tag = product.find('div', class_='image-container')
                product_data['product_image'] = img_tag.find('img')['src'] if img_tag and img_tag.find('img') and img_tag.find('img').get('src') else None

                ratings_div = product.find('div', class_='auc-product-tile__bazaarvoice--ratings')
                product_data['product_ratings_id'] = ratings_div.get('data-bv-product-id') if ratings_div else None

                product_labels = [{'alt': label.get('alt'), 'title': label.get('title')} for label in product.find_all('img', class_='auc-product-labels__icon')]
                product_data['product_labels'] = json.dumps(product_labels) if product_labels else None

                promo_div = product.find('div', class_='auc-price__promotion__label')
                product_data['product_promotions'] = promo_div.text.strip() if promo_div else None

                products_data.append(product_data)

            except (AttributeError, KeyError, TypeError) as e:
                pid = product.get('data-pid', 'N/A')
                logger.error(f"Error parsing details for product PID {pid}: {e}", exc_info=False)

    except Exception as e:
        logger.error(f"General error parsing HTML content: {e}", exc_info=True)
    return products_data
# --- Stage 4: Category Data Processing ---

def process_single_category(
    category_task: Dict[str, str],
    base_url: str,
    sz: int,
    pmin: str,
    headers: Dict[str, str],
    logger: logging.Logger,
    metrics_collector: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """ Processes a single category: fetches pages, parses products, returns list of dicts. """
    cgid = category_task['cgid']
    all_category_products = []
    logger.info(f"Processing category: {cgid}")
    total_products_for_category = None
    pages_processed = 0

    try:
        # Pass metrics_collector to the fetcher generator
        html_generator = fetch_all_html_for_category(
            category_task=category_task, base_url=base_url, sz=sz, pmin=pmin,
            headers=headers, logger=logger, metrics_collector=metrics_collector
        )

        for html_content, start_index in html_generator:
            pages_processed += 1
            if start_index == 0: # First page
                # Attempt to parse total products count (can return None)
                total_products_for_category = parse_total_products(html_content, logger)
                if total_products_for_category is None:
                     logger.warning(f"Could not determine total products for {cgid}. Will continue page by page.")
                else:
                     logger.info(f"Determined total products for {cgid}: {total_products_for_category}")

            if not html_content:
                logger.warning(f"Received empty HTML content for {cgid} at start index {start_index}. Stopping category.")
                break

            parsed_page_products = parse_products_from_html(html_content, logger)
            logger.debug(f"Parsed {len(parsed_page_products)} products from page starting at {start_index} for {cgid}")

            if not parsed_page_products and start_index > 0:
                 # If a subsequent page has no products, assume it's the end (or an error)
                 logger.info(f"No products found on page starting at {start_index} for {cgid}. Assuming end of results.")
                 break

            all_category_products.extend(parsed_page_products)

            # Check stopping condition based on total_products, if known
            if total_products_for_category is not None and start_index + sz >= total_products_for_category:
                logger.info(f"Reached expected end for {cgid} based on total product count ({total_products_for_category}).")
                break
            # Safety break if it runs too long without a total count
            if total_products_for_category is None and pages_processed > 100: # Adjust limit as needed
                 logger.warning(f"Processed {pages_processed} pages for {cgid} without finding total count. Stopping to prevent infinite loop.")
                 break


        logger.info(f"Finished processing category {cgid}. Found {len(all_category_products)} products across {pages_processed} pages.")

    except Exception as e:
        logger.error(f"Failed to process category {cgid} due to an error: {e}", exc_info=True)
        # Return whatever was collected so far
    return all_category_products


# --- Stage 5: Data Aggregation & Enrichment ---
def aggregate_results(list_of_category_results: List[List[Dict[str, Any]]], logger: logging.Logger) -> Optional[pd.DataFrame]:
    """ Aggregates results from all categories into a single DataFrame. """
    all_products = [prod for res in list_of_category_results for prod in res] # Flatten
    if not all_products:
        logger.warning("No products found across all categories.")
        return None
    logger.info(f"Aggregating {len(all_products)} total products into DataFrame.")
    return pd.DataFrame(all_products)


def enrich_and_optimize_data(df: pd.DataFrame, scrape_timestamp: datetime, source_name: str, logger: logging.Logger) -> pd.DataFrame:
    """ Adds metadata columns and optimizes DataFrame data types. """
    logger.info("Enriching and optimizing DataFrame.")
    df['scrape_timestamp'] = scrape_timestamp.strftime("%Y-%m-%d %H:%M:%S")
    df['source'] = source_name

    # Infer object types better
    df = df.infer_objects()

    # Ensure string columns are strings and handle None/NaN
    str_cols = ['product_id', 'product_name', 'image_url', 'price_per_unit',
                'unit_quantity_info', 'product_link', 'promotion', 'variant', 'dimension3']
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).replace({'nan': None, 'None': None})

    # Reorder columns for readability (optional)
    cols_order = ['scrape_timestamp', 'source', 'product_id', 'product_name', 'brand', 'category',
                  'price', 'price_per_unit', 'unit_quantity_info', 'promotion', 'variant', 'dimension3',
                  'product_link', 'image_url']
    # Include only columns that actually exist in the DataFrame
    existing_cols_order = [col for col in cols_order if col in df.columns]
    # Add any remaining columns not in the preferred order
    remaining_cols = [col for col in df.columns if col not in existing_cols_order]
    df = df[existing_cols_order + remaining_cols]


    logger.info("DataFrame enrichment and optimization complete.")
    logger.info(f"Final DataFrame memory usage:\n{df.memory_usage(deep=True)}")
    logger.info(f"Final DataFrame dtypes:\n{df.dtypes}")
    return df


# --- Stage 6: Data Persistence (Local Save) ---
def save_to_parquet(df: pd.DataFrame, directory: str, filename: str, logger: logging.Logger, compression: str = 'snappy') -> Optional[str]:
    """ Saves the DataFrame to a local Parquet file. Returns file path or None. """
    try:
        os.makedirs(directory, exist_ok=True)
        file_path = os.path.join(directory, filename)
        logger.info(f"Saving data ({len(df)} rows) locally to Parquet file: {file_path}")
        df.to_parquet(file_path, compression=compression, index=False, engine='pyarrow')
        logger.info(f"Successfully saved data locally to {file_path}")
        return file_path
    except Exception as e:
        logger.error(f"Failed to save data locally to Parquet file {file_path}: {e}", exc_info=True)
        return None

# --- Stage 7: Metrics Persistence ---
def save_metrics_to_json(metrics_list: List[Dict[str, Any]], directory: str, filename: str, logger: logging.Logger) -> Optional[str]:
    """ Saves the calculated metrics to a JSON file. Returns file path or None. """
    try:
        os.makedirs(directory, exist_ok=True)
        file_path = os.path.join(directory, filename)
        logger.info(f"Saving metrics to JSON file: {file_path}")
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(metrics_list, f, indent=4)
        logger.info(f"Successfully saved metrics to {file_path}")
        return file_path
    except Exception as e:
        logger.error(f"Failed to save metrics to JSON file {file_path}: {e}", exc_info=True)
        return None

# --- Stage 8: Orchestration (Main Pipeline Runner) ---

def run_continente_pipeline(
    category_list: List[str],
    base_url: str = BASE_URL_CONTINENTE,
    sz: int = DEFAULT_SZ_CONTINENTE,
    pmin: str = DEFAULT_PMIN_CONTINENTE,
    headers: Dict[str, str] = DEFAULT_HEADERS_CONTINENTE,
    output_dir: str = OUTPUT_DIR_CONTINENTE,
    log_dir: str = LOG_DIR_CONTINENTE,
    metrics_dir: str = METRICS_DIR_CONTINENTE,
    upload_to_s3: bool = True
):
    """ Runs the full Continente data scraping pipeline. """
    start_time = datetime.now()
    # Use a specific logger prefix for Continente
    logger = setup_logger(log_dir, "continente_pipeline")
    logger.info("--- Continente Scraping Pipeline Started ---")
    logger.info(f"Configuration: PageSize={sz}, Pmin={pmin}, LocalOutputDir={output_dir}, LogDir={log_dir}, MetricsDir={metrics_dir}")
    logger.info(f"Processing {len(category_list)} categories: {category_list}")
    logger.info(f"Upload to Supabase S3 enabled: {upload_to_s3}")

    # --- METRICS: Initialize collector ---
    metrics_collector = {"total_num_api_calls": 0}
    # --- End Metrics ---

    # 1. Generate Tasks
    category_tasks = generate_category_tasks(category_list)
    logger.info(f"Generated {len(category_tasks)} category tasks.")

    # 2. Process each category
    all_results = []
    # Use tqdm for progress bar
    with tqdm(total=len(category_tasks), desc="Processing Categories") as pbar:
        for task in category_tasks:
            cgid = task['cgid']
            pbar.set_description(f"Processing {cgid}")
            try:
                # Pass logger and metrics_collector explicitly
                category_products = process_single_category(
                    category_task=task, base_url=base_url, sz=sz, pmin=pmin,
                    headers=headers, logger=logger, metrics_collector=metrics_collector
                )
                if category_products:
                    # Add cgid to each product dictionary *before* aggregation
                    for prod in category_products:
                        prod['cgid_source'] = cgid # Add source category ID
                    all_results.append(category_products)
                    # Logging is now inside process_single_category
                else:
                    logger.info(f"No products found for category {cgid}.")
            except Exception as e:
                logger.error(f"Critical error processing category task {cgid}: {e}", exc_info=True)
            pbar.update(1)
            # Optional slight delay between categories if needed
            # time.sleep(random.uniform(1.0, 3.0))

    # 3. Aggregate Results
    final_df = aggregate_results(all_results, logger)

    # --- METRICS: Prepare final metrics list ---
    final_metrics_list = []
    final_metrics_list.append({"metric_name": "total_num_api_calls", "metric_value": metrics_collector.get("total_num_api_calls", 0)})
    # --- End Metrics ---

    if final_df is None or final_df.empty:
        logger.warning("Aggregation resulted in an empty DataFrame. Skipping enrichment, save, and upload.")
        # Add empty/null metrics
        final_metrics_list.extend([
            {"metric_name": "num_rows", "metric_value": 0},
            {"metric_name": "data_size_raw_bytes", "metric_value": 0},
            {"metric_name": "data_size_compressed_bytes", "metric_value": None},
            {"metric_name": "estimated_compression_ratio", "metric_value": None}
        ])
    else:
        # 4. Enrich and Optimize Data
        final_df = enrich_and_optimize_data(final_df, scrape_timestamp=start_time, source_name="Continente", logger=logger)

        # --- METRICS: Calculate DataFrame-dependent metrics ---
        num_rows = len(final_df)
        data_size_raw = final_df.memory_usage(deep=True).sum()
        final_metrics_list.append({"metric_name": "num_rows", "metric_value": num_rows})
        final_metrics_list.append({"metric_name": "data_size_raw_bytes", "metric_value": data_size_raw})
        # --- End Metrics ---

        # 5. Save Data Locally
        timestamp_str_file = start_time.strftime("%Y%m%d_%H%M%S")
        output_filename = f"continente_products_{timestamp_str_file}.parquet"
        local_file_path = save_to_parquet(final_df, output_dir, output_filename, logger)

        # --- METRICS: Calculate file-dependent metrics ---
        data_size_compressed = None
        compression_ratio = None
        if local_file_path and os.path.exists(local_file_path):
            try:
                data_size_compressed = os.path.getsize(local_file_path)
                if data_size_raw > 0:
                    compression_ratio = round(data_size_compressed / data_size_raw, 4)
            except Exception as e:
                logger.error(f"Could not get size of saved file {local_file_path}: {e}")
        final_metrics_list.append({"metric_name": "data_size_compressed_bytes", "metric_value": data_size_compressed})
        final_metrics_list.append({"metric_name": "estimated_compression_ratio", "metric_value": compression_ratio})
        # --- End Metrics ---

        # 6. Upload to Supabase S3 (if enabled and local save succeeded)
        if upload_to_s3 and local_file_path:
            # Define Supabase folder structure (consistent with Auchan)
            timestamp_str_folder = start_time.strftime("%Y%m%d")
            supabase_folder = f"processed/continente/{timestamp_str_folder}" # Changed raw to processed
            try:
                # Use the placeholder/actual upload function
                upload_parquet_to_supabase_s3(
                    logger=logger,
                    file_path=local_file_path,
                    folder_name=supabase_folder
                )
                # Optional: Delete local file after successful upload
                # os.remove(local_file_path)
                # logger.info(f"Removed local file: {local_file_path}")
            except Exception as e:
                logger.error(f"Supabase upload step failed: {e}", exc_info=True)

    # --- METRICS: Calculate final time and save all metrics ---
    end_time = datetime.now()
    total_elapsed_time = end_time - start_time
    total_seconds = round(total_elapsed_time.total_seconds(), 2)
    final_metrics_list.append({"metric_name": "total_elapsed_time_seconds", "metric_value": total_seconds})

    # Sort list alphabetically by metric name
    final_metrics_list.sort(key=lambda x: x['metric_name'])

    # Save metrics JSON
    metrics_filename = f"continente_metrics_{start_time.strftime('%Y%m%d_%H%M%S')}.json"
    save_metrics_to_json(final_metrics_list, metrics_dir, metrics_filename, logger)
    # --- End Metrics ---

    logger.info(f"Pipeline completed in {total_elapsed_time}.")
    if final_df is not None and not final_df.empty:
        logger.info(f"Total products collected and processed: {len(final_df)}")
    else:
        logger.info("No products were processed.")
    logger.info(f"Metrics saved to {os.path.join(metrics_dir, metrics_filename)}")
    logger.info("--- Continente Scraping Pipeline Finished ---")


# --- Main Execution ---
if __name__ == "__main__":
    # Ensure category names are accurate cgid values from Continente website
    run_continente_pipeline(
        category_list=CATEGORIES_CONTINENTE,
        upload_to_s3=False # Set to False to skip S3 upload
        # Optional: Override defaults like page size if needed
        # sz=36,
    )