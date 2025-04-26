import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import re
import time
import random
from datetime import datetime
import os
import io # For potential direct DataFrame upload

# Import necessary libraries
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger # Use Prefect's logger
from prefect.tasks import task_input_hash # Optional: for caching based on inputs

# Import utility functions (assuming they are in utils.py)
from utils import retry_on_failure, upload_csv_to_gcs



# --- Environment Variable Loading ---
# Load environment variables from .env file
load_dotenv()

# --- Task Definitions ---
# Note: Using Prefect's built-in retries is often cleaner, but
# we'll keep your custom decorator for minimal changes as requested.

@task # No caching specified here, handled by fetch_page if needed
def parse_total_products(html_content):
    logger = get_run_logger()
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        counter_div = soup.find(
            "div",
            class_="search-results-products-counter d-flex justify-content-center")
        if counter_div and counter_div.text:
            numbers = re.findall(r'\d+', counter_div.text)
            if numbers:
                number_products = max(map(int, numbers))
                logger.debug(f"Parsed total products: {number_products}")
                return number_products
        logger.warning("Could not parse total products from HTML.")
        return None
    except Exception as e:
        logger.error(f"Error parsing total products: {e}", exc_info=True)
        return None


@task # No caching specified here
def parse_product_data(html_content, cgid):
    logger = get_run_logger()
    product_data = []
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        product_tiles = soup.find_all("div", class_="product-tile")
        logger.debug(f"Found {len(product_tiles)} product tiles for cgid={cgid}")

        for tile in product_tiles:
            name, product_id, price_per_kg, brand, category = "", "", 0.0, "", ""
            image_url, price_per_unit, min_quantity, product_link = "", None, None, ""

            product_info_json_str = tile.get("data-product-tile-impression")
            if product_info_json_str:
                try:
                    # Basic cleaning: replace potentially problematic single quotes if needed
                    # This might need refinement based on actual JSON structure issues
                    cleaned_json_str = product_info_json_str.replace("'", '"') # Simple replacement, might break if ' is inside values
                    # Attempt to load JSON
                    product_info = json.loads(cleaned_json_str)

                    name = product_info.get("name", "")
                    product_id = product_info.get("id", "")
                    price_per_kg = product_info.get("price", 0.0) # Assuming this is price, not price/kg necessarily
                    brand = product_info.get("brand", "")
                    category = product_info.get("category", "")

                except json.JSONDecodeError as json_e:
                    # Log the problematic JSON string for debugging
                    logger.warning(f"JSONDecodeError for cgid={cgid}, tile data: {product_info_json_str}. Error: {json_e}")
                    # Fallback or skip - decided to continue with empty fields

            image_tag = tile.find("img", class_="ct-tile-image")
            image_url = image_tag.get("data-src", image_tag.get("src")) if image_tag else "" # Check data-src first, then src

            price_per_unit_tag = tile.find("div", class_="pwc-tile--price-secondary")
            price_per_unit = price_per_unit_tag.get_text(strip=True) if price_per_unit_tag else None

            min_quantity_tag = tile.find("p", class_="pwc-tile--quantity")
            min_quantity = min_quantity_tag.get_text(strip=True) if min_quantity_tag else None

            product_link_tag = tile.find("a", href=True)
            # Make link absolute if it's relative
            if product_link_tag and product_link_tag.get("href"):
                 link = product_link_tag["href"]
                 if link.startswith('/'):
                     product_link = f"https://www.continente.pt{link}"
                 else:
                     product_link = link
            else:
                 product_link = ""


            product_data.append({
                "Product Name": name,
                "Product ID": product_id,
                "Price": price_per_kg,
                "Price per unit": price_per_unit,
                "Brand": brand,
                "Category": category, # This comes from JSON, might be broad
                "Image URL": image_url,
                "Minimum Quantity": min_quantity,
                "Product Link": product_link
            })

        df = pd.DataFrame(product_data)
        df["cgid"] = cgid # Add the requested category id
        logger.info(f"Parsed {len(df)} products for cgid={cgid}")
        return df

    except Exception as e:
        logger.error(f"Error parsing product data for cgid={cgid}: {e}", exc_info=True)
        # Return empty DataFrame on error to avoid breaking the flow
        return pd.DataFrame(product_data)


# Use Prefect's caching mechanism based on input hash
# Cache results in GCS if configured
@task(retries=3, retry_delay_seconds=120, cache_key_fn=task_input_hash)
# @retry_on_failure(retries=3, delay=120) # Using Prefect's retries instead
def fetch_page(start, sz, cgid, pmin, srule):
    """Fetches a single page of product results."""
    logger = get_run_logger()
    url = "https://www.continente.pt/on/demandware.store/Sites-continente-Site/default/Search-UpdateGrid"
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br", # Removed zstd for broader compatibility, requests handles it
        "Accept-Language": "en-US,en;q=0.5", # Changed to EN to potentially avoid locale issues
        "Connection": "keep-alive",
        "Host": "www.continente.pt",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36", # Common User Agent
        "X-Requested-With": "XMLHttpRequest", # Often required for AJAX updates
    }
    params = {
        "cgid": cgid,
        "pmin": pmin,
        # "srule": srule, # srule might be specific and cause issues, disabling for now
        "start": start,
        "sz": sz,
    }
    logger.info(f"Fetching page: cgid={cgid}, start={start}, sz={sz}")
    try:
        response = requests.get(url, params=params, headers=headers, timeout=30) # Added timeout
        response.raise_for_status()
        logger.debug(f"Successfully fetched page: cgid={cgid}, start={start}. Status: {response.status_code}")
        # It's generally better to return the content rather than writing to a file here
        # with open('data.html', 'w', encoding='utf-8') as f: # Added encoding
        #     f.write(response.text)
        return response.text
    except requests.exceptions.RequestException as e:
        logger.error(f"HTTP Error fetching page: cgid={cgid}, start={start}. Error: {e}", exc_info=True)
        raise # Re-raise to trigger Prefect's retry mechanism


@task # Using default Prefect retries on the sub-task fetch_page
# @retry_on_failure(retries=3, delay=360) # Retries handled by fetch_page task
def fetch_all_products_for_category(cgid, sz=72, pmin="0.01", srule=None): # Smaller batch size, removed default srule
    """Fetches all product data for a given category ID."""
    logger = get_run_logger()
    logger.info(f"Starting fetch for category: {cgid}")
    all_products_df = pd.DataFrame()
    current_start = 0
    total_products = None
    max_products_to_fetch = 10000 # Safety limit to prevent infinite loops

    # Initial fetch to get total count
    try:
        logger.info(f"Fetching initial page for category {cgid} to get total count.")
        initial_html_content = fetch_page(start=0, sz=sz, cgid=cgid, pmin=pmin, srule=srule)
        if initial_html_content:
            total_products = parse_total_products(initial_html_content)
            if total_products:
                 logger.info(f"Total products estimated for category {cgid}: {total_products}")
                 # Process the first page of products
                 page_df = parse_product_data(initial_html_content, cgid)
                 if not page_df.empty:
                     all_products_df = pd.concat([all_products_df, page_df], ignore_index=True)
                 current_start += sz
            else:
                logger.warning(f"Could not determine total products for {cgid}. Scraping first page only.")
                # Attempt to parse products even if total count failed
                page_df = parse_product_data(initial_html_content, cgid)
                if not page_df.empty:
                    all_products_df = pd.concat([all_products_df, page_df], ignore_index=True)
                total_products = len(all_products_df) # Set total to what we found
        else:
             logger.error(f"Initial fetch failed for category {cgid}. Aborting category.")
             return pd.DataFrame() # Return empty df

    except Exception as e:
        logger.error(f"Error during initial fetch for category {cgid}: {e}", exc_info=True)
        return pd.DataFrame() # Return empty df if initial fetch fails critically

    # Loop if we have a total count and haven't reached it yet
    while total_products is not None and current_start < total_products and current_start < max_products_to_fetch:
        try:
            logger.info(f"Fetching next page for {cgid}: start={current_start}, sz={sz}")
            html_content = fetch_page(current_start, sz, cgid, pmin, srule)

            if not html_content:
                logger.warning(f"Received empty content for {cgid} at start={current_start}. Stopping loop.")
                break # Stop if fetch fails or returns nothing

            page_df = parse_product_data(html_content, cgid)

            if page_df.empty:
                logger.warning(f"No products parsed on page starting at {current_start} for {cgid}. Might be end or issue.")
                # Optional: break here if receiving empty pages consistently indicates the end
                # break

            all_products_df = pd.concat([all_products_df, page_df], ignore_index=True)
            logger.info(f"Fetched {len(all_products_df)} / {total_products} products for category {cgid}")

            current_start += sz
            delay = random.randint(3, 7) # Shorter delay, adjust as needed
            logger.debug(f"Waiting for {delay} seconds before next request for {cgid}")
            time.sleep(delay)

        except Exception as e:
            logger.error(f"Error fetching subsequent page for category {cgid} at start={current_start}: {e}", exc_info=True)
            # Decide whether to break or continue
            break # Let's break on error to avoid hammering

    if current_start >= max_products_to_fetch:
        logger.warning(f"Reached max fetch limit ({max_products_to_fetch}) for category {cgid}.")

    # Add metadata before returning
    if not all_products_df.empty:
        all_products_df["tracking_date"] = datetime.now().strftime("%Y-%m-%d")
        all_products_df["source"] = "Continente"

    logger.info(f"Completed fetching for category {cgid}. Total products retrieved: {len(all_products_df)}")
    return all_products_df


# --- Main Flow Definition ---

@flow(name="Continente Category Scraper")
def process_and_save_categories():
    """
    Main Prefect flow to scrape product data for specified Continente categories,
    save results locally, and upload them to Google Cloud Storage.
    """
    logger = get_run_logger()
    logger.info("Starting Continente category scraper flow.")

    # --- Configuration Loading ---
    gcs_bucket_name = os.getenv("GCS_BUCKET_NAME")
    base_data_path = os.getenv("GCS_SUBFOLDER_PATH", "data/raw/continente") # Default local path
    base_data_path = os.path.join(base_data_path, "continente")

    # GCP Credentials Dictionary (from .env)
    credentials_dict = None
    logger.info("GOOGLE_APPLICATION_CREDENTIALS path not set, attempting to load credentials from direct ENV VARS.")
    try:
        private_key = os.getenv("PRIVATE_KEY")
        if private_key:
                # Replace literal '\n' with actual newlines for JSON parsing
                private_key = private_key.replace('\\n', '\n')

        credentials_dict = {
            "type": os.getenv("TYPE"),
            "project_id": os.getenv("PROJECT_ID"),
            "private_key_id": os.getenv("PRIVATE_KEY_ID"),
            "private_key": private_key,
            "client_email": os.getenv("CLIENT_EMAIL"),
            "client_id": os.getenv("CLIENT_ID"),
            "auth_uri": os.getenv("AUTH_URI"),
            "token_uri": os.getenv("TOKEN_URI"),
            "auth_provider_x509_cert_url": os.getenv("AUTH_PROVIDER_X509_CERT_URL"),
            "client_x509_cert_url": os.getenv("CLIENT_X509_CERT_URL"),
            "universe_domain": os.getenv("UNIVERSE_DOMAIN")
        }
        # Validate that essential keys are present
        if not all([credentials_dict["project_id"], credentials_dict["private_key"], credentials_dict["client_email"]]):
                logger.warning("Missing essential GCP credential components in environment variables. Upload might rely on ADC.")
                credentials_dict = None # Invalidate dict if incomplete
        else:
                logger.info("Successfully loaded GCP credentials from direct ENV VARS.")

    except Exception as e:
        logger.warning(f"Could not assemble credentials dictionary from environment variables: {e}. Upload might rely on ADC.")
        credentials_dict = None


    if not gcs_bucket_name:
        logger.error("GCS_BUCKET_NAME environment variable not set. Cannot upload results.")
        # Decide if flow should fail or just skip upload
        # raise ValueError("GCS_BUCKET_NAME not set.") # Option: Fail the flow

    # --- Setup Paths ---
    run_date_str = datetime.now().strftime("%Y%m%d")
    local_run_path = os.path.join(base_data_path, run_date_str)
    gcs_run_folder = f"retail_data/continente/{run_date_str}" # GCS path prefix

    # Ensure the local base path exists; Prefect usually runs in isolated envs, but good practice.
    try:
        os.makedirs(local_run_path, exist_ok=True)
        logger.info(f"Local data directory '{local_run_path}' ensured.")
    except OSError as e:
        logger.error(f"Could not create local directory {local_run_path}: {e}")
        # Decide if flow should fail
        # raise # Option: Fail if local storage is critical

    # --- Category Processing ---
    CATEGORIES = [
        "congelados", "frescos", "mercearias", "bebidas", "biologicos",
        "limpeza", "higiene-beleza", "bebe"
    ]

    # Optional: Hit the base URL once to potentially establish session cookies if needed
    try:
        initial_url = "https://www.continente.pt/"
        requests.get(initial_url, timeout=15)
        logger.info(f"Initial check/hit to {initial_url} successful.")
    except requests.exceptions.RequestException as e:
        logger.warning(f"Could not hit initial URL {initial_url}: {e}. Proceeding anyway.")

    all_category_results = {} # Store results per category

    for category in CATEGORIES:
        logger.info(f"--- Processing category: {category} ---")
        # Use .submit() for potential (but limited by GIL) parallelism if desired
        # future = fetch_all_products_for_category.submit(category)
        # df_category_products = future.result()
        df_category_products = fetch_all_products_for_category(category) # Sequential execution

        all_category_results[category] = df_category_products # Store DataFrame

        if not df_category_products.empty:
            filename = f"{category}_{run_date_str}.csv" # Add date to filename
            local_file_path = os.path.join(local_run_path, filename)

            # --- Save Locally ---
            try:
                df_category_products.to_csv(local_file_path, index=False)
                logger.info(f"Saved data locally for '{category}' to {local_file_path}")
            except IOError as e:
                logger.error(f"Failed to save {local_file_path}: {e}")
                # Continue to upload attempt?

            # --- Upload to GCS ---
            if gcs_bucket_name:
                gcs_destination_blob = f"{gcs_run_folder}/{filename}"
                logger.info(f"Attempting to upload {filename} to gs://{gcs_bucket_name}/{gcs_destination_blob}")

                # Decide upload method: from file or directly from DataFrame
                # Method 1: Upload from the saved file
                success = upload_csv_to_gcs(
                    logger=logger,
                    file_path=local_file_path,
                    folder_name=gcs_run_folder, # Pass the folder structure within the bucket
                    gcs_bucket_name=gcs_bucket_name,
                    credentials_dict=credentials_dict # Pass dict if available
                )

                if success:
                        logger.info(f"Successfully uploaded '{filename}' for category '{category}' to GCS.")
                else:
                        logger.error(f"Failed to upload '{filename}' for category '{category}' to GCS.")
                        # Optional: Mark flow as failed?
            else:
                logger.warning(f"Skipping GCS upload for {category} because GCS_BUCKET_NAME is not set.")

        else:
            logger.warning(f"No data retrieved for category '{category}'. Nothing to save or upload.")


    logger.info("--- Completed processing all categories ---")

    # Optional: Return summary or combined results
    summary = {cat: len(df) for cat, df in all_category_results.items()}
    logger.info(f"Processing Summary (Product Counts): {summary}")
    return summary


# --- Script Execution ---
if __name__ == "__main__":
    # This allows running the flow directly from the script
    process_and_save_categories()