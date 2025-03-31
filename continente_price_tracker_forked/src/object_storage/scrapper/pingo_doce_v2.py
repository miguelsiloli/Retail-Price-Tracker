import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import re
import time
import random
from datetime import datetime, timedelta
import os
import sys
import logging
from functools import wraps
from typing import List, Dict, Any, Optional, Generator, Tuple
from tqdm import tqdm # Add tqdm for progress bars
# from utils import retry_on_failure

# --- Configuration ---
BASE_URL_PINGODOCE = "https://www.pingodoce.pt/produtos/marca-propria-pingo-doce/pingo-doce/"
# Note: The URL suggests it ONLY gets "pingo-doce" brand. Verify if this is intended.
# If you need other products, the base URL and category structure might differ.

DEFAULT_HEADERS_PINGODOCE = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
    "Host": "www.pingodoce.pt",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Upgrade-Insecure-Requests": "1",
    "TE": "trailers"
}
# Pingo Doce uses 'cp' param apparently as page number, not postal code based on original code
DEFAULT_PAGE_PARAM_KEY = "cp" # Parameter used for pagination

RETRY_CONFIG_PINGODOCE = {"retries": 3, "delay": 60} # Delay in seconds
POLITENESS_DELAY_PINGODOCE = 3 # Fixed delay between requests (seconds)

# Output Directories
OUTPUT_DIR_PINGODOCE = "data/processed/pingo_doce"
LOG_DIR_PINGODOCE = "logs"
METRICS_DIR_PINGODOCE = "data/metrics/pingo_doce"

# List of categories (param values) to fetch
# These look like URL slugs, verify they are the correct 'categoria' parameter values
CATEGORIES_PINGODOCE = [
    "pingo-doce-lacticinios", "pingo-doce-bebidas",
    "pingo-doce-frescos-embalados", "pingo-doce-higiene-e-beleza",
    "pingo-doce-maquinas-e-capsulas-de-cafe", "pingo-doce-mercearia",
    "pingo-doce-refeicoes-prontas", "pingo-doce-cozinha-e-limpeza",
    "pingo-doce-congelados"
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

# --- Utility Functions ---
def setup_logger(log_directory: str, filename_prefix: str = "pingodoce_pipeline") -> logging.Logger:
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
    """ Generates a list of task dictionaries based on the 'categoria' parameter values. """
    return [{"categoria": category} for category in category_list]

# --- Stage 2: HTML Fetching ---

@retry_on_failure(retries=RETRY_CONFIG_PINGODOCE["retries"], delay=RETRY_CONFIG_PINGODOCE["delay"])
def fetch_single_page_html(
    url: str,
    params: Dict[str, Any],
    headers: Dict[str, str],
    logger: logging.Logger
) -> str:
    """ Fetches HTML content for a single page request using the retry decorator. """
    try:
        # logger.debug(f"Fetching URL: {url} with params: {params}")
        response = requests.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        logger.debug(f"Successfully fetched {response.url}")
        # API call count incremented by caller
        return response.text
    except requests.exceptions.Timeout:
        logger.warning(f"Timeout occurred fetching {url} with params {params}")
        raise
    except requests.exceptions.RequestException as e:
        logger.warning(f"Request failed for {url} with params {params}: {e}")
        raise e
    except Exception as e:
        logger.error(f"Non-request exception during fetch for {url}: {e}", exc_info=True)
        raise e

# --- Stage 3: HTML Parsing ---

def parse_last_page_number(html_content: str, logger: logging.Logger) -> Optional[int]:
    """ Parses the HTML to determine the last page number from pagination controls. """
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        # Find elements likely containing page numbers
        page_elements = soup.find_all('div', class_='page js-change-page') # Original class
        if not page_elements:
             # Add fallbacks if class name changes
             page_elements = soup.select('ul.pagination li a[data-page]') # Example common pattern

        page_numbers = []
        for page_element in page_elements:
            page_num_str = page_element.get('data-page')
            if page_num_str and page_num_str.isdigit():
                page_numbers.append(int(page_num_str))

        if page_numbers:
            last_page = max(page_numbers)
            logger.debug(f"Extracted last page number: {last_page}")
            return last_page
        else:
            logger.debug("No page number elements found or parsed.")
            return 1 # Assume 1 page if no pagination found
    except Exception as e:
        logger.error(f"Error parsing last page number: {e}", exc_info=True)
        return None # Return None on error to signal failure

def parse_products_from_html(html_content: str, logger: logging.Logger) -> List[Dict[str, Any]]:
    """ Parses HTML content to extract product details into a list of dictionaries. """
    products_list = []
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        # Find all product card containers
        products = soup.find_all('div', class_='product-cards') # Verify class name

        if not products:
            logger.debug("No 'product-cards' divs found on the page.")
            return []

        for product_div in products:
            product_data = {}
            try:
                # Product URL and ID
                link_tag = product_div.find('a', class_='product-cards__link')
                product_url = link_tag['href'] if link_tag else None
                if product_url:
                    product_data['product_url'] = product_url
                    # Extract ID from URL (assuming format /.../id/name)
                    parts = product_url.strip('/').split('/')
                    product_data['product_id'] = parts[-2] if len(parts) >= 2 else None
                else:
                    product_data['product_url'] = None
                    product_data['product_id'] = None
                    logger.warning("Could not find product link/ID for a card.")
                    # Continue if ID is crucial, or try alternative ID sources if available

                # Product Name
                name_tag = product_div.find('h3', class_='product-cards__title')
                product_data['product_name'] = name_tag.text.strip() if name_tag else None

                # Product Price
                price_tag = product_div.find('span', class_='product-cards_price')
                # Extract only the number, handle currency symbols and separators
                if price_tag:
                    price_text = price_tag.text.strip()
                    # Regex to find float/int number, handles ',' or '.' as decimal sep
                    match = re.search(r'[\d.,]+', price_text)
                    if match:
                        try:
                            price_str = match.group(0).replace('.', '').replace(',', '.') # Normalize to '.' decimal
                            product_data['product_price'] = float(price_str)
                        except ValueError:
                            logger.warning(f"Could not convert price string '{match.group(0)}' to float for product {product_data.get('product_id')}")
                            product_data['product_price'] = None
                    else:
                         product_data['product_price'] = None
                else:
                    product_data['product_price'] = None

                # Product Rating (optional)
                try:
                    rating_tag = product_div.find('div', class_='bv_text') # BazaarVoice rating text
                    product_data['product_rating'] = rating_tag.text.strip() if rating_tag else None
                except Exception: # Catch any error during rating find/parse
                    product_data['product_rating'] = None

                # Add other fields if needed and selectors are known (e.g., image, unit price)
                # image_tag = product_div.find('img', class_='product-cards__image')
                # product_data['product_image'] = image_tag['src'] if image_tag else None

                products_list.append(product_data)

            except (AttributeError, KeyError, TypeError, IndexError) as e:
                 logger.error(f"Error parsing details for one product card: {e}", exc_info=False)
                 # Continue to the next product card

    except Exception as e:
        logger.error(f"General error parsing products from HTML: {e}", exc_info=True)

    return products_list


# --- Stage 4: Category Data Processing ---

def process_single_category(
    category_task: Dict[str, str],
    base_url: str,
    page_param_key: str,
    headers: Dict[str, str],
    logger: logging.Logger,
    metrics_collector: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """ Processes a single category: fetches pages, parses products, returns list of dicts. """
    categoria = category_task['categoria']
    all_category_products = []
    logger.info(f"Processing category: {categoria}")
    last_page = None

    # Default payload values
    payload_base = {
        "q": "",
        "o": "maisbaixo", # Sorting
        "categoria": categoria,
        "subcategorias": "",
        "filtros": "",
        page_param_key: 1, # Start with page 1
        "novidades": 0
    }

    try:
        # 1. Fetch page 1
        logger.debug(f"Fetching page 1 for {categoria}")
        page_1_payload = payload_base.copy()
        html_page_1 = fetch_single_page_html(base_url, params=page_1_payload, headers=headers, logger=logger)
        metrics_collector["total_num_api_calls"] = metrics_collector.get("total_num_api_calls", 0) + 1

        # 2. Parse last page number from page 1
        last_page = parse_last_page_number(html_page_1, logger)
        if last_page is None:
            logger.error(f"Failed to determine last page number for category {categoria}. Aborting category.")
            return [] # Cannot proceed without knowing page count
        logger.info(f"Determined last page for {categoria}: {last_page}")

        # 3. Parse products from page 1
        products_page_1 = parse_products_from_html(html_page_1, logger)
        logger.debug(f"Parsed {len(products_page_1)} products from page 1 for {categoria}")
        all_category_products.extend(products_page_1)

        # 4. Fetch and parse remaining pages (if last_page > 1)
        if last_page > 1:
            for page_num in range(2, last_page + 1):
                logger.debug(f"Fetching page {page_num} of {last_page} for category {categoria}")
                page_payload = payload_base.copy()
                page_payload[page_param_key] = page_num

                try:
                    html_content = fetch_single_page_html(base_url, params=page_payload, headers=headers, logger=logger)
                    metrics_collector["total_num_api_calls"] = metrics_collector.get("total_num_api_calls", 0) + 1

                    products_df = parse_products_from_html(html_content, logger)
                    logger.debug(f"Parsed {len(products_df)} products from page {page_num} for {categoria}")
                    if not products_df and page_num < last_page:
                         logger.warning(f"Page {page_num} for {categoria} returned no products. Might be end or error.")
                         # Optionally break here if desired, or continue to try all pages

                    all_category_products.extend(products_df)

                except Exception as e:
                    logger.error(f"Error processing page {page_num} for category {categoria}: {e}", exc_info=True)
                    # Optionally break or continue based on desired error tolerance

                # Politeness delay
                logger.debug(f"Waiting {POLITENESS_DELAY_PINGODOCE} seconds before next request")
                time.sleep(POLITENESS_DELAY_PINGODOCE)

        logger.info(f"Finished processing category {categoria}. Found {len(all_category_products)} products across {last_page} page(s).")

    except Exception as e:
        logger.error(f"Failed to process category {categoria} due to an error (likely page 1 fetch/parse): {e}", exc_info=True)
        # Return whatever was collected before the failure (might be empty)
    return all_category_products


# --- Stage 5: Data Aggregation & Enrichment ---
def aggregate_results(list_of_category_results: List[List[Dict[str, Any]]], logger: logging.Logger) -> Optional[pd.DataFrame]:
    """ Aggregates results from all categories into a single DataFrame. """
    all_products = [prod for res in list_of_category_results for prod in res]
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

    # Ensure string columns are strings and handle None/NaN
    str_cols = ['product_id', 'product_name', 'product_url', 'product_rating']
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).replace({'nan': None, 'None': None})

    # Reorder columns (optional)
    cols_order = ['scrape_timestamp', 'source', 'product_id', 'product_name',
                  'product_price', 'product_rating', 'product_url']
    existing_cols_order = [col for col in cols_order if col in df.columns]
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
        logger.info(metrics_list)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(metrics_list, f, indent=4)
        logger.info(f"Successfully saved metrics to {file_path}")
        return file_path
    except Exception as e:
        logger.error(f"Failed to save metrics to JSON file {file_path}: {e}", exc_info=True)
        return None

# --- Stage 8: Orchestration (Main Pipeline Runner) ---

def run_pingodoce_pipeline(
    category_list: List[str],
    base_url: str = BASE_URL_PINGODOCE,
    page_param_key: str = DEFAULT_PAGE_PARAM_KEY,
    headers: Dict[str, str] = DEFAULT_HEADERS_PINGODOCE,
    output_dir: str = OUTPUT_DIR_PINGODOCE,
    log_dir: str = LOG_DIR_PINGODOCE,
    metrics_dir: str = METRICS_DIR_PINGODOCE,
    upload_to_s3: bool = True
):
    """ Runs the full Pingo Doce data scraping pipeline. """
    start_time = datetime.now()
    logger = setup_logger(log_dir, "pingodoce_pipeline")
    logger.info("--- Pingo Doce Scraping Pipeline Started ---")
    logger.info(f"Configuration: BaseURL={base_url}, LocalOutputDir={output_dir}, LogDir={log_dir}, MetricsDir={metrics_dir}")
    logger.info(f"Processing {len(category_list)} categories: {category_list}")
    logger.info(f"Upload to Supabase/B2 enabled: {upload_to_s3}")

    # --- METRICS: Initialize collector ---
    metrics_collector = {"total_num_api_calls": 0}
    # --- End Metrics ---

    # 1. Generate Tasks
    category_tasks = generate_category_tasks(category_list)
    logger.info(f"Generated {len(category_tasks)} category tasks.")

    # 2. Process each category
    all_results = []
    with tqdm(total=len(category_tasks), desc="Processing Categories") as pbar:
        for task in category_tasks:
            categoria = task['categoria']
            pbar.set_description(f"Processing {categoria}")
            try:
                # Pass logger and metrics_collector
                category_products = process_single_category(
                    category_task=task, base_url=base_url, page_param_key=page_param_key,
                    headers=headers, logger=logger, metrics_collector=metrics_collector
                )
                if category_products:
                    # Add source category info before aggregation
                    for prod in category_products:
                        prod['categoria_source'] = categoria
                    all_results.append(category_products)
                    # Logging is inside process_single_category
                else:
                    logger.warning(f"No products returned for category {categoria}.")
            except Exception as e:
                logger.error(f"Critical error processing category task {categoria}: {e}", exc_info=True)
            pbar.update(1)

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
        final_df = enrich_and_optimize_data(final_df, scrape_timestamp=start_time, source_name="pingo-doce", logger=logger)

        # --- METRICS: Calculate DataFrame-dependent metrics ---
        num_rows = len(final_df)
        data_size_raw = final_df.memory_usage(deep=True).sum()
        final_metrics_list.append({"metric_name": "num_rows", "metric_value": num_rows})
        final_metrics_list.append({"metric_name": "data_size_raw_bytes", "metric_value": data_size_raw.item()})
        # --- End Metrics ---

        # 5. Save Data Locally
        timestamp_str_file = start_time.strftime("%Y%m%d_%H%M%S")
        output_filename = f"pingodoce_products_{timestamp_str_file}.parquet"
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
        final_metrics_list.append({"metric_name": "estimated_compression_ratio", "metric_value": compression_ratio.item()})
        # --- End Metrics ---

        # 6. Upload to Supabase S3/B2 (if enabled and local save succeeded)
        if upload_to_s3 and local_file_path:
            timestamp_str_folder = start_time.strftime("%Y%m%d")
            # Adjust path as needed (raw vs processed)
            supabase_folder = f"processed/pingo_doce/{timestamp_str_folder}"
            try:
                # Use the actual upload function (ensure it's imported/defined correctly)
                success = upload_parquet_to_supabase_s3(
                    logger=logger,
                    file_path=local_file_path,
                    folder_name=supabase_folder
                )
                if not success:
                    logger.warning(f"Upload failed for {local_file_path}")
                # Optional: Delete local file after successful upload
                # if success: os.remove(local_file_path); logger.info(f"Removed local file: {local_file_path}")
            except Exception as e:
                logger.error(f"Supabase/B2 upload step failed unexpectedly: {e}", exc_info=True)

    # --- METRICS: Calculate final time and save all metrics ---
    end_time = datetime.now()
    total_elapsed_time = end_time - start_time
    total_seconds = round(total_elapsed_time.total_seconds(), 2)
    final_metrics_list.append({"metric_name": "total_elapsed_time_seconds", "metric_value": total_seconds})

    # Sort list alphabetically by metric name
    final_metrics_list.sort(key=lambda x: x['metric_name'])

    # Save metrics JSON
    metrics_filename = f"pingodoce_metrics_{start_time.strftime('%Y%m%d_%H%M%S')}.json"
    save_metrics_to_json(final_metrics_list, metrics_dir, metrics_filename, logger)
    # --- End Metrics ---

    logger.info(f"Pipeline completed in {total_elapsed_time}.")
    if final_df is not None and not final_df.empty:
        logger.info(f"Total products collected and processed: {len(final_df)}")
    else:
        logger.info("No products were processed.")
    logger.info(f"Metrics saved to {os.path.join(metrics_dir, metrics_filename)}")
    logger.info("--- Pingo Doce Scraping Pipeline Finished ---")

# --- Main Execution ---
if __name__ == "__main__":

    # Ensure category names are accurate 'categoria' parameter values for Pingo Doce
    run_pingodoce_pipeline(
        category_list=CATEGORIES_PINGODOCE,
        upload_to_s3=False # Set to False to skip S3 upload
    )