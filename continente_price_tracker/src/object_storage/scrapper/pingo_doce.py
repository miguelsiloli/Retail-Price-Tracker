from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sys
import os

# this is only for testing purposes in VM

src_path = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
sys.path.append(src_path)

from utils import upload_csv_to_gcs
import time
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
import random

@task(retries=3, retry_delay_seconds=60, cache_key_fn=task_input_hash)
def fetch_html_from_pingodoce(cp, categoria):
    """
    Fetches the HTML content for a specific category page from the Pingo Doce website.

    Parameters:
    - cp (int): The postal code used to filter the products.
    - categoria (str): The category of products to fetch (e.g., "pingo-doce-lacticinios").

    Returns:
    - str: The HTML content of the page.

    Raises:
    - requests.exceptions.HTTPError: If the request to the server fails (non-200 status code).

    Example:
    >>> html_content = fetch_html_from_pingodoce(cp=1000, categoria="pingo-doce-lacticinios")
    >>> print(html_content)  # Prints the HTML content of the category page.
    """
    # Get Prefect logger for this task run
    logger = get_run_logger()
    url = "https://www.pingodoce.pt/produtos/marca-propria-pingo-doce/pingo-doce/"
    payload = {
        "q": "",
        "o": "maisbaixo",
        "categoria": categoria,
        "subcategorias": "",
        "filtros": "",
        "cp": cp,
        "novidades": 0
    }
    logger.debug(f"Fetching HTML for categoria={categoria}, cp={cp}") # Example log
    response = requests.get(url, params=payload)

    if response.status_code == 200:
        logger.debug(f"Successfully fetched HTML for categoria={categoria}, cp={cp}") # Example log
        return response.text
    else:
        logger.error(f"HTTP Error {response.status_code} for categoria={categoria}, cp={cp}") # Example log
        response.raise_for_status()

@task
def parse_last_page(html_content):
    """
    Parses the HTML content to determine the last page number of the product listings.

    Parameters:
    - html_content (str): The HTML content of the page to parse.

    Returns:
    - int: The last page number of the product listings (e.g., 5).
    - None: If the last page cannot be determined (e.g., no pagination).

    Example:
    >>> last_page = parse_last_page(html_content)
    >>> print(last_page)  # Prints the last page number (e.g., 5).
    """
    # Get Prefect logger for this task run
    logger = get_run_logger()
    soup = BeautifulSoup(html_content, 'html.parser')

    # Find all elements with the class 'page js-change-page'
    pages = soup.find_all('div', class_='page js-change-page')

    if pages:
        try:
            last_page = pages[-1]['data-page']
            logger.debug(f"Found last page: {last_page}") # Example log
            return int(last_page)
        except (KeyError, IndexError, ValueError) as e:
            logger.warning(f"Could not extract last page number: {e}") # Example log
            return None # Or return 1 depending on desired fallback
    else:
        logger.debug("No pagination elements found.") # Example log
        return None # Or return 1


@task
def parse_products_from_html(html_content):
    """
    Parses the HTML content to extract product details, including ID, name, price, image URL, and rating.

    Parameters:
    - html_content (str): The HTML content of the page to parse.

    Returns:
    - pd.DataFrame: A pandas DataFrame containing product details such as product ID, name, price,
      image URL, and rating.

    Example:
    >>> products_df = parse_products_from_html(html_content)
    >>> print(products_df.head())  # Prints the first few rows of the parsed product DataFrame.
    """
    # Get Prefect logger for this task run
    logger = get_run_logger()
    soup = BeautifulSoup(html_content, 'html.parser')

    product_schema = {
        "product_id": pd.Series(dtype='str'),
        "product_name": pd.Series(dtype='str'),
        "product_price": pd.Series(dtype='str'),
        # "product_image": pd.Series(dtype='str'),
        "product_url": pd.Series(dtype='str'),
        "product_rating": pd.Series(dtype='str')
    }

    product_df = pd.DataFrame(product_schema)
    products = soup.find_all('div', class_='product-cards')
    logger.debug(f"Found {len(products)} product cards on page.") # Example log

    product_list = []

    for product in products:
        product_data = {}
        try:
            # Extract product details
            product_url = product.find('a', class_='product-cards__link')['href']
            product_data['product_url'] = product_url
            product_id = product_url.split('/')[-2]
            product_data['product_id'] = product_id
            product_name = product.find(
                'h3', class_='product-cards__title').text.strip()
            product_data['product_name'] = product_name
            product_price = product.find(
                'span', class_='product-cards_price').text.strip()
            product_data['product_price'] = product_price
            # product_image = product.find('img',
            #                              class_='product-cards__image')['src']
            # product_data['product_image'] = product_image

            try:
                product_rating = product.find('div', class_='bv_text').text.strip()
                product_data['product_rating'] = product_rating
            except:
                product_data['product_rating'] = None

            product_list.append(product_data)
        except Exception as e:
             logger.warning(f"Could not parse a product card: {e}", exc_info=False) # Log card error

    # Convert list of products to DataFrame
    if product_list: # Check if list is not empty
         product_df = pd.concat([product_df, pd.DataFrame(product_list)],
                                ignore_index=True)
         product_df = product_df.reindex(columns=product_schema.keys())
    else:
         # Ensure an empty DataFrame with the correct columns is returned if no products parsed
         product_df = pd.DataFrame(columns=product_schema.keys()).astype(product_schema)


    # Assert can cause hard failures, consider removing or making optional in production
    # assert list(product_df.columns) == list(
    #     product_schema.keys()), "DataFrame structure does not match the schema"
    if list(product_df.columns) != list(product_schema.keys()):
        logger.warning("DataFrame columns do not match schema definition.")

    logger.debug(f"Parsed {len(product_df)} products from HTML.") # Example log
    return product_df


# NOTE: Removed the global logger setup line: logger = setup_logger(...)

@task
def parse_all_pages_for_category(categoria):
    """
    Fetches and parses all pages for a specific category on the Pingo Doce website.
    """
    # Get Prefect logger for this task run
    logger = get_run_logger()
    logger.info(f"Starting to parse all pages for category: {categoria}")

    # Use .submit() if running tasks concurrently, otherwise call directly
    first_page_html = fetch_html_from_pingodoce(cp=1, categoria=categoria) # Direct call assumes sequential

    if not first_page_html:
        logger.error(f"Failed to fetch initial page for category {categoria}. Aborting category.")
        return pd.DataFrame() # Return empty DataFrame

    last_page = parse_last_page(first_page_html) # Direct call

    if last_page is None:
        last_page = 1
        logger.warning(f"Could not determine last page for category {categoria}. Assuming only 1 page.")
    else:
        logger.info(f"Found {last_page} pages for category {categoria}")

    all_products_df = pd.DataFrame()

    # Parse first page content if available
    try:
        products_first_page_df = parse_products_from_html(first_page_html)
        if not products_first_page_df.empty:
            all_products_df = pd.concat([all_products_df, products_first_page_df], ignore_index=True)
            logger.info(f"Parsed first page for category {categoria}. Products so far: {len(all_products_df)}")
    except Exception as e:
        logger.error(f"Error parsing first page for category {categoria}: {str(e)}", exc_info=True)
        # Decide if should continue or abort

    # Loop starting from page 2 if last_page > 1
    for cp in range(2, last_page + 1):
        logger.debug(f"Fetching page {cp} of {last_page} for category {categoria}")
        try:
            html_content = fetch_html_from_pingodoce(cp, categoria) # Direct call
            if not html_content:
                 logger.warning(f"No content received for page {cp}, category {categoria}. Skipping.")
                 continue

            products_df = parse_products_from_html(html_content) # Direct call
            if not products_df.empty:
                all_products_df = pd.concat([all_products_df, products_df], ignore_index=True)
                logger.info(f"Successfully parsed page {cp} for category {categoria}. Total products so far: {len(all_products_df)}")
            else:
                 logger.info(f"No products found on page {cp} for category {categoria}.")

        except Exception as e:
            # Log error, but continue loop (retries handled by fetch task)
            logger.error(f"Error processing page {cp} for category {categoria}: {str(e)}", exc_info=True)

        # Use a small random delay
        sleep_time = random.uniform(1, 4)
        logger.debug(f"Waiting {sleep_time:.2f} seconds before next request")
        time.sleep(sleep_time)


    if not all_products_df.empty:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        all_products_df["source"] = "pingo-doce"
        all_products_df["timestamp"] = timestamp

    logger.info(f"Completed parsing all pages for category {categoria}. Total products: {len(all_products_df)}")
    return all_products_df



@flow(name="Pingo Doce Category Scraper")
def parse_and_save_all_categories(categories, base_path="data/raw/pingo_doce"):
    """
    Parses and saves the product data for multiple categories as CSV files.
    Also uploads the files to Supabase storage.
    """
    logger = get_run_logger()
    logger.info(f"Starting to parse and save data for {len(categories)} categories")

    # Get GCS bucket name from environment variable
    gcs_bucket_name = os.getenv("GCS_BUCKET_NAME")
    
    # Create credentials dictionary from environment variables
    credentials_dict = {
        "type": os.getenv("TYPE"),
        "project_id": os.getenv("PROJECT_ID"),
        "private_key_id": os.getenv("PRIVATE_KEY_ID"),
        "private_key": os.getenv("PRIVATE_KEY").replace("\\n", "\n") if os.getenv("PRIVATE_KEY") else None,
        "client_email": os.getenv("CLIENT_EMAIL"),
        "client_id": os.getenv("CLIENT_ID"),
        "auth_uri": os.getenv("AUTH_URI"),
        "token_uri": os.getenv("TOKEN_URI"),
        "auth_provider_x509_cert_url": os.getenv("AUTH_PROVIDER_X509_CERT_URL"),
        "client_x509_cert_url": os.getenv("CLIENT_X509_CERT_URL"),
        "universe_domain": os.getenv("UNIVERSE_DOMAIN")
    }

    base_path = base_path + "/" + datetime.now().strftime("%Y%m%d")

    if not os.path.exists(base_path):
        os.makedirs(base_path)
        logger.info(f"Created directory: {base_path}")

    supabase_folder = f"retail_data/pingo_doce/{datetime.now().strftime('%Y%m%d')}"

    for categoria in categories:
        logger.info(f"Processing category: {categoria}")
        try:
            all_products_df = parse_all_pages_for_category(categoria)

            if not all_products_df.empty:
                csv_filename = f"{categoria.replace(' ', '_')}.csv"
                file_path = os.path.join(base_path, csv_filename)

                all_products_df.to_csv(file_path, index=False)
                logger.info(f"Saved data for category '{categoria}' to '{file_path}'. Total products: {len(all_products_df)}")

                # Upload to Supabase
                # upload_csv_to_supabase_s3(logger = logger, 
                #                             file_path = file_path, 
                #                             folder_name = supabase_folder)
                
                upload_csv_to_gcs(logger = logger, 
                                file_path = file_path, 
                                folder_name = supabase_folder,
                                gcs_bucket_name=gcs_bucket_name,
                                credentials_dict=credentials_dict)
            else:
                logger.warning(f"No data found for category '{categoria}'. Skipping...")
        except Exception as e:
            logger.error(f"Error processing category {categoria}: {str(e)}", exc_info=True)

    logger.info("Completed parsing and saving data for all categories")
