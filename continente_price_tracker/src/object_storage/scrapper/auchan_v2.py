import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import os
import pandas as pd
import time
import json
from datetime import datetime, timedelta
import logging
from functools import wraps
from typing import List, Dict, Any, Optional, Generator, Tuple

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
BASE_URL = "https://www.auchan.pt/on/demandware.store/Sites-AuchanPT-Site/pt_PT/Search-UpdateGrid"
DEFAULT_SZ = 48
DEFAULT_PREFN1 = "brand"
DEFAULT_PREFV1 = ""
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "TE": "Trailers"
}
RETRY_CONFIG = {"retries": 3, "delay": 60}
POLITENESS_DELAY = 3
OUTPUT_DIR = "data/processed/auchan"
LOG_DIR = "logs"
METRICS_DIR = "data/metrics/auchan" # Directory for metrics JSON

CGID_LIST = [
    "alimentacao-", "biologico-e-escolhas-alimentares",
    "limpeza-da-casa-e-roupa", "bebidas-e-garrafeira", "marcas-auchan"
]

# --- Main Execution ---


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

# --- Utility Functions ---

def setup_logger(log_directory: str, filename_prefix: str = "auchan_pipeline") -> logging.Logger:
    """Sets up a logger that writes to both console and file."""
    os.makedirs(log_directory, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"{filename_prefix}_{timestamp}.log"
    log_path = os.path.join(log_directory, log_filename)

    logger = logging.getLogger(filename_prefix)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(module)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.INFO)
        logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(logging.INFO)
        logger.addHandler(console_handler)

    return logger

# --- Stage 1: Task Generation --- (Unchanged)
def generate_category_tasks(cgid_list: List[str], prefn1: str, prefv1: str) -> List[Dict[str, str]]:
    """Generates a list of task dictionaries, one for each category ID."""
    tasks = []
    for cgid in cgid_list:
        tasks.append({
            "cgid": cgid,
            "prefn1": prefn1,
            "prefv1": prefv1
        })
    return tasks

# --- Stage 2: HTML Fetching ---

@retry_on_failure(retries=RETRY_CONFIG["retries"], delay=RETRY_CONFIG["delay"])
def fetch_single_page_html(
    url: str,
    params: Dict[str, Any],
    headers: Dict[str, str],
    logger: logging.Logger
) -> str:
    """ Fetches HTML content for a single page request using the retry decorator from utils. """
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        logger.debug(f"Successfully fetched {response.url}")
        # Do NOT increment API call count here - do it in the caller after success
        return response.text
    except requests.exceptions.RequestException as e:
        raise e # Re-raise for decorator
    except Exception as e:
        logger.error(f"Non-request exception during fetch for {url}: {e}", exc_info=True)
        raise e


def fetch_all_pages_for_category(
    category_task: Dict[str, str],
    base_url: str,
    sz: int,
    headers: Dict[str, str],
    logger: logging.Logger,
    metrics_collector: Dict[str, Any] # Added metrics dictionary
) -> Generator[Tuple[str, int], None, None]:
    """
    Fetches HTML content for all pages of a specific category, yielding page content
    and incrementing the API call counter in metrics_collector.
    """
    start = 0
    cgid = category_task['cgid']
    prefn1 = category_task['prefn1']
    prefv1 = category_task['prefv1']

    logger.info(f"Starting fetch for category: {cgid}, page size: {sz}")

    while True:
        params = {
            "cgid": cgid, "prefn1": prefn1, "prefv1": prefv1,
            "start": start, "sz": sz, "next": "true"
        }
        logger.info(f"Fetching page for {cgid}, start index: {start}")

        try:
            html_content = fetch_single_page_html(base_url, params=params, headers=headers, logger=logger)
            # --- METRICS: Increment API call count AFTER successful fetch ---
            metrics_collector["total_num_api_calls"] = metrics_collector.get("total_num_api_calls", 0) + 1
            # --- End Metrics ---
            yield html_content, start
            start += sz
            time.sleep(POLITENESS_DELAY)

        except requests.exceptions.RequestException as e:
            logger.error(f"Permanent error fetching page for {cgid} at start={start} (after retries): {e}", exc_info=False)
            break # Stop fetching for this category
        except Exception as e:
             logger.error(f"Unexpected error during fetch loop for {cgid} at start={start}: {e}", exc_info=True)
             break

# --- Stage 3: HTML Parsing --- (Unchanged)
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
                        category_data = json.loads(category_str)
                        product_data['product_category'] = category_data.get('item_category')
                        product_data['product_category2'] = category_data.get('item_category2')
                        product_data['product_category3'] = category_data.get('item_category3')
                    except json.JSONDecodeError:
                        logger.warning(f"Could not parse category JSON for PID {product_data.get('product_id', 'N/A')}")

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
    headers: Dict[str, str],
    logger: logging.Logger,
    metrics_collector: Dict[str, Any] # Added metrics dictionary
) -> List[Dict[str, Any]]:
    """
    Processes a single category by fetching all its pages and parsing products,
    passing the metrics_collector down.
    """
    cgid = category_task['cgid']
    all_category_products = []
    logger.info(f"Processing category: {cgid}")

    try:
        # Pass metrics_collector to the fetcher
        html_generator = fetch_all_pages_for_category(
            category_task=category_task, base_url=base_url, sz=sz,
            headers=headers, logger=logger, metrics_collector=metrics_collector
        )

        last_page_found = False
        for html_content, start_index in html_generator:
            if not html_content: break # Should not happen normally

            parsed_page_products = parse_products_from_html(html_content, logger)
            logger.debug(f"Parsed {len(parsed_page_products)} products from page starting at {start_index} for {cgid}")

            if not parsed_page_products:
                logger.info(f"No products found on page starting at {start_index} for {cgid}. Assuming end.")
                last_page_found = True
                break

            all_category_products.extend(parsed_page_products)

            if len(parsed_page_products) < sz:
                logger.info(f"Received {len(parsed_page_products)} (<{sz}) products for {cgid}. Assuming last page.")
                last_page_found = True
                break

        log_msg = f"Finished processing category {cgid}. Found {len(all_category_products)} products."
        if not last_page_found and len(all_category_products) > 0:
             log_msg += " (Last page condition not explicitly met)."
        logger.info(log_msg)

    except Exception as e:
        logger.error(f"Failed to process category {cgid} due to an error: {e}", exc_info=True)
    return all_category_products


# --- Stage 5: Data Aggregation & Enrichment --- (Unchanged)
def aggregate_results(list_of_category_results: List[List[Dict[str, Any]]], logger: logging.Logger) -> Optional[pd.DataFrame]:
    """ Aggregates results from all categories into a single DataFrame. """
    all_products = [prod for res in list_of_category_results for prod in res] # Flatten list
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
    df = df.infer_objects()

    str_cols = ['product_id', 'product_name', 'product_urls', 'product_image',
                'product_ratings_id', 'product_labels', 'product_promotions',
                'product_category_raw']
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).replace({'nan': None, 'None': None})

    logger.info("DataFrame enrichment and optimization complete.")
    # Raw data size metric will be calculated in the main pipeline function
    return df


# --- Stage 6: Data Persistence (Local Save) --- (Unchanged)
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

# --- NEW Stage: Metrics Persistence ---
def save_metrics_to_json(metrics_list: List[Dict[str, Any]], directory: str, filename: str, logger: logging.Logger) -> Optional[str]:
    """
    Saves the calculated metrics to a JSON file.

    Args:
        metrics_list: List of dictionaries, each with 'metric_name' and 'metric_value'.
        directory: The directory to save the file in.
        filename: The name for the output JSON file.
        logger: The logger instance.

    Returns:
        The full path to the saved file if successful, otherwise None.
    """
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


# --- Stage 7: Orchestration (Main Pipeline Runner) ---

def run_auchan_pipeline(
    cgid_list: List[str],
    base_url: str = BASE_URL,
    sz: int = DEFAULT_SZ,
    prefn1: str = DEFAULT_PREFN1,
    prefv1: str = DEFAULT_PREFV1,
    headers: Dict[str, str] = DEFAULT_HEADERS,
    output_dir: str = OUTPUT_DIR,
    log_dir: str = LOG_DIR,
    metrics_dir: str = METRICS_DIR, # Added metrics dir
    upload_to_s3: bool = True
):
    """ Runs the full Auchan data scraping pipeline, including optional S3 upload and metrics generation. """
    start_time = datetime.now()
    logger = setup_logger(log_dir, "auchan_pipeline")
    logger.info("--- Auchan Scraping Pipeline Started ---")
    logger.info(f"Configuration: PageSize={sz}, LocalOutputDir={output_dir}, LogDir={log_dir}, MetricsDir={metrics_dir}")
    logger.info(f"Processing {len(cgid_list)} categories: {cgid_list}")
    logger.info(f"Upload to Supabase S3 enabled: {upload_to_s3}")

    # --- METRICS: Initialize collector ---
    metrics_collector = {"total_num_api_calls": 0}
    # --- End Metrics ---

    # 1. Generate Tasks
    category_tasks = generate_category_tasks(cgid_list, prefn1, prefv1)
    logger.info(f"Generated {len(category_tasks)} category tasks.")

    # 2. Process each category
    all_results = []
    with tqdm(total=len(category_tasks), desc="Processing Categories") as pbar:
        for task in category_tasks:
            cgid = task['cgid']
            pbar.set_description(f"Processing {cgid}")
            try:
                # Pass logger and metrics_collector explicitly
                category_products = process_single_category(
                    category_task=task, base_url=base_url, sz=sz,
                    headers=headers, logger=logger, metrics_collector=metrics_collector
                )
                if category_products: all_results.append(category_products)
                # Logging now happens inside process_single_category
            except Exception as e:
                logger.error(f"Critical error processing category task {cgid}: {e}", exc_info=True)
            pbar.update(1)

    # 3. Aggregate Results
    final_df = aggregate_results(all_results, logger)

    # --- METRICS: Prepare final metrics list ---
    final_metrics_list = []
    final_metrics_list.append({"metric_name": "total_num_api_calls", "metric_value": metrics_collector.get("total_num_api_calls", 0)})
    # --- End Metrics ---

    if final_df is None or final_df.empty:
        logger.warning("Aggregation resulted in an empty DataFrame. Skipping enrichment, save, and upload.")
        final_metrics_list.append({"metric_name": "num_rows", "metric_value": 0})
        final_metrics_list.append({"metric_name": "data_size_raw_bytes", "metric_value": 0})
        final_metrics_list.append({"metric_name": "data_size_compressed_bytes", "metric_value": None})
        final_metrics_list.append({"metric_name": "estimated_compression_ratio", "metric_value": None})
    else:
        # 4. Enrich and Optimize Data
        final_df = enrich_and_optimize_data(final_df, scrape_timestamp=start_time, source_name="auchan", logger=logger)

        # --- METRICS: Calculate DataFrame-dependent metrics ---
        num_rows = len(final_df)
        data_size_raw = final_df.memory_usage(deep=True).sum()
        final_metrics_list.append({"metric_name": "num_rows", "metric_value": num_rows})
        final_metrics_list.append({"metric_name": "data_size_raw_bytes", "metric_value": data_size_raw})
        # --- End Metrics ---

        # 5. Save Data Locally
        timestamp_str_file = start_time.strftime("%Y%m%d_%H%M%S")
        output_filename = f"auchan_products_{timestamp_str_file}.parquet"
        local_file_path = save_to_parquet(final_df, output_dir, output_filename, logger)

        # --- METRICS: Calculate file-dependent metrics ---
        data_size_compressed = None
        compression_ratio = None
        if local_file_path and os.path.exists(local_file_path):
            try:
                data_size_compressed = os.path.getsize(local_file_path)
                if data_size_raw > 0: # Avoid division by zero
                    compression_ratio = round(data_size_compressed / data_size_raw, 4)
                else:
                     compression_ratio = None # Or perhaps 0 or 1 depending on definition
                logger.info(f"Compressed file size: {data_size_compressed} bytes. Raw estimate: {data_size_raw} bytes.")
            except Exception as e:
                logger.error(f"Could not get size of saved file {local_file_path}: {e}")
        final_metrics_list.append({"metric_name": "data_size_compressed_bytes", "metric_value": data_size_compressed})
        final_metrics_list.append({"metric_name": "estimated_compression_ratio", "metric_value": compression_ratio.item()})
        # --- End Metrics ---

        # 6. Upload to Supabase S3 (if enabled and local save succeeded)
        if upload_to_s3 and local_file_path:
            timestamp_str_folder = start_time.strftime("%Y%m%d")
            supabase_folder = f"raw/auchan/{timestamp_str_folder}"
            try:
                upload_parquet_to_supabase_s3(
                    logger=logger, file_path=local_file_path, folder_name=supabase_folder
                )
            except Exception as e:
                logger.error(f"Supabase upload step failed: {e}", exc_info=True)

    # --- METRICS: Calculate final time and save all metrics ---
    end_time = datetime.now()
    total_elapsed_time = end_time - start_time
    total_seconds = round(total_elapsed_time.total_seconds(), 2)
    final_metrics_list.append({"metric_name": "total_elapsed_time_seconds", "metric_value": total_seconds})

    # Sort list alphabetically by metric name for consistency
    final_metrics_list.sort(key=lambda x: x['metric_name'])

    metrics_filename = f"auchan_metrics_{start_time.strftime('%Y%m%d_%H%M%S')}.json"
    save_metrics_to_json(final_metrics_list, metrics_dir, metrics_filename, logger)
    # --- End Metrics ---

    logger.info(f"Pipeline completed in {total_elapsed_time}.")
    if final_df is not None and not final_df.empty:
        logger.info(f"Total products collected and processed: {len(final_df)}")
    else:
        logger.info("No products were processed.")
    logger.info(f"Metrics saved to {os.path.join(metrics_dir, metrics_filename)}")
    logger.info("--- Auchan Scraping Pipeline Finished ---")

if __name__ == "__main__":
    run_auchan_pipeline(
        cgid_list=CGID_LIST,
        upload_to_s3=False # Control S3 upload
    )