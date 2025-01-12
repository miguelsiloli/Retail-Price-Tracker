import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import os
import pandas as pd
import time
import json
from datetime import datetime
from utils import retry_on_failure, upload_csv_to_supabase_s3
from logger import setup_logger


def parse_products_from_html(html_content):
    """
    Parses HTML content to extract product information and returns it as a DataFrame.

    Args:
        html_content (str): The raw HTML content of the page to be parsed.

    Returns:
        pd.DataFrame: A DataFrame containing parsed product information, such as product ID, name, price, categories, image, and other attributes.
    """
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
        # "quantity_selector": pd.Series(dtype='str')
    }

    # Create an empty DataFrame with the defined schema
    product_df = pd.DataFrame(product_schema)

    # Find all product elements in the HTML
    products = soup.find_all('div', class_='product')

    product_list = []

    for product in products:
        product_data = {}

        # Extract product ID
        product_data['product_id'] = product['data-pid']

        # Extract product URLs
        product_urls = product.find('div', class_='product-tile')['data-urls']
        product_data['product_urls'] = product_urls

        # Extract product name
        product_name = product.find('div',
                                    class_='pdp-link').find('a').text.strip()
        product_data['product_name'] = product_name

        # Extract product price
        product_price = product.find('span', class_='value')['content']
        product_data['product_price'] = float(product_price)

        # Extract product category
        product_category = product.find('div',
                                        class_='product-tile')['data-gtm-new']
        product_data['product_category'] = product_category

        # Extract nested product categories
        product_category_data = json.loads(product_category)
        product_data['product_category'] = product_category_data.get(
            'item_category', None)
        product_data['product_category2'] = product_category_data.get(
            'item_category2', None)
        product_data['product_category3'] = product_category_data.get(
            'item_category3', None)

        # Extract product image URL
        product_image = product.find(
            'div', class_='image-container').find('img')['src']
        product_data['product_image'] = product_image

        # Extract product ratings
        product_ratings = product.find(
            'div', class_='auc-product-tile__bazaarvoice--ratings'
        )['data-bv-product-id']
        product_data['product_ratings'] = product_ratings

        # Extract product labels
        product_labels = []
        labels = product.find_all('img', class_='auc-product-labels__icon')
        for label in labels:
            product_labels.append({
                'alt': label['alt'],
                'title': label['title'],
                # 'src': label['src']
            })
        product_data['product_labels'] = product_labels

        # Extract product promotions (assign None if not found)
        product_promotions = product.find('div',
                                          class_='auc-price__promotion__label')
        product_data['product_promotions'] = product_promotions.text.strip(
        ) if product_promotions else None

        # Extract quantity selector details (assign None if not found)
        # quantity_selector = product.find('div', class_='auc-qty-selector')
        # if quantity_selector:
        #     product_data['quantity_selector'] = quantity_selector
        # else:
        #     product_data['quantity_selector'] = None

        # Convert nested dictionaries to JSON strings for DataFrame compatibility
        product_data["product_urls"] = str(product_data["product_urls"])
        product_data["product_ratings"] = str(product_data["product_ratings"])
        product_data["product_labels"] = str(product_data["product_labels"])
        # product_data["quantity_selector"] = str(
        #     product_data["quantity_selector"])

        # Append the product data to the list
        product_list.append(product_data)

    # Convert the list of product data to a DataFrame
    product_df = pd.concat([product_df, pd.DataFrame(product_list)],
                           ignore_index=True)

    # Reindex the DataFrame to ensure it has the same columns as the schema
    product_df = product_df.reindex(columns=product_schema.keys())

    # Assert that the DataFrame structure is consistent with the schema
    assert list(product_df.columns) == list(
        product_schema.keys()), "DataFrame structure does not match the schema"

    return product_df


@retry_on_failure(retries=3, delay=60)
def get_auchan_data(cgid, prefn1, prefv1, start, sz, next, selectedUrl):
    """
    Fetches HTML data from the Auchan store's search API endpoint.

    Args:
        cgid (str): The category group ID used for filtering the products.
        prefn1 (str): The name of the first preference filter.
        prefv1 (str): The value of the first preference filter.
        start (int): The starting index for the product list.
        sz (int): The number of products to fetch per request.
        next (str): Whether or not to fetch the next page of products (usually "true").
        selectedUrl (str): The URL that includes the parameters for the request.

    Returns:
        str: The raw HTML content from the Auchan store's search results.
    """
    url = "https://www.auchan.pt/on/demandware.store/Sites-AuchanPT-Site/pt_PT/Search-UpdateGrid"
    headers = {
        "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36",
        "Accept":
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
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
        "next": next
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        return response.text
    else:
        response.raise_for_status()


@retry_on_failure(retries=3, delay=60)
def get_and_parse_auchan_data(cgid, prefn1, prefv1, sz, base_url, logger):
    """
    Retrieves and parses product data from the Auchan store in a paginated manner.

    Args:
        cgid (str): The category group ID used for filtering the products.
        prefn1 (str): The name of the first preference filter.
        prefv1 (str): The value of the first preference filter.
        sz (int): The number of products to fetch per request.
        base_url (str): The base URL of the search results.
        logger (logging.Logger): The logger object for logging messages.

    Returns:
        pd.DataFrame: A DataFrame containing parsed product information across multiple pages.
    """
    start = 0
    all_data = pd.DataFrame()

    with tqdm(total=30, unit='batch') as pbar:
        while True:
            selectedUrl = f"{base_url}?cgid={cgid}&prefn1={prefn1}&prefv1={prefv1}&start={start}&sz={sz}&next=true"

            try:
                data = get_auchan_data(cgid, prefn1, prefv1, start, sz, "true", selectedUrl)
                logger.info(f"Successful GET request for URL: {selectedUrl}")
                parsed_data = parse_products_from_html(data)

            except Exception as e:
                logger.error(f"Error fetching data for URL {selectedUrl}: {str(e)}")
                return all_data

            all_data = pd.concat([all_data, parsed_data], ignore_index=True)

            pbar.update(1)
            time.sleep(3)

            if len(parsed_data) < sz:
                break

            start += sz
            pbar.total += 1  # Increase the total count dynamically

    return all_data


def save_data_for_all_cgids(cgid_list,
                            prefn1,
                            prefv1,
                            sz,
                            base_url,
                            base_path="data/raw"):
    """
    Fetches and processes product data for each cgid in the list, saves it to CSV files, 
    and uploads the files to Supabase storage.

    Args:
        cgid_list (list): List of cgid values for which data will be fetched.
        prefn1 (str): The first filter parameter for fetching data.
        prefv1 (str): The value corresponding to the prefn1 filter.
        sz (int): The number of items to fetch per request.
        base_url (str): The base URL for fetching data.
        base_path (str): The directory where the CSV files will be saved. Defaults to "data".
    """
    # Create a timestamp for unique filenames and logging
    timestamp = datetime.now().strftime("%Y%m%d")

    # Set up logging
    log_directory = "logs"
    os.makedirs(log_directory, exist_ok=True)
    log_path = os.path.join(log_directory, f"auchan.log")
    logger = setup_logger(log_path)

    logger.info(f"Starting data fetch process for {len(cgid_list)} cgids")

    # Ensure the base path exists; if not, create it
    if not os.path.exists(base_path):
        os.makedirs(base_path)
        logger.info(f"Directory '{base_path}' created.")

    # Create a subdirectory with timestamp if base_path doesn't exist
    data_directory = os.path.join(base_path, timestamp)
    os.makedirs(data_directory, exist_ok=True)
    logger.info(f"Data will be saved in '{data_directory}'")

    # Supabase folder for uploads
    supabase_folder = f"raw/auchan/{timestamp}"

    # Loop through each cgid and fetch & save the corresponding data
    for cgid in cgid_list:
        logger.info(f"Processing cgid: {cgid}")

        try:
            # Fetch and parse the data for the given cgid
            final_data = get_and_parse_auchan_data(cgid, prefn1, prefv1, sz, base_url, logger)
            final_data["source"] = "auchan"
            final_data["timestamp"] = timestamp

            if not final_data.empty:
                # Create a filename with timestamp and cgid
                filename = f"{cgid}_{timestamp}.csv"
                file_path = os.path.join(data_directory, filename)

                # Save the data to CSV
                final_data.to_csv(file_path, index=False)
                logger.info(f"Data for {cgid} saved to {file_path}")

                # Upload to Supabase
                upload_csv_to_supabase_s3(
                    logger=logger,
                    file_path=file_path,
                    folder_name=supabase_folder
                )
            else:
                logger.warning(f"No data found for {cgid}. Skipping...")
        except Exception as e:
            logger.error(f"Error processing cgid {cgid}: {str(e)}", exc_info=True)

    logger.info("Data fetch process completed")


# List of cgid values
# cgid_list = [
#     "alimentacao-", "biologico-e-escolhas-alimentares",
#     "limpeza-da-casa-e-roupa", "bebidas-e-garrafeira", "marcas-auchan"
# ]
