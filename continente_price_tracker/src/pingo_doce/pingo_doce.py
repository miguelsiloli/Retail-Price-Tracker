from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sys
import os
from logger import setup_logger

# this is only for testing purposes in VM

src_path = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
sys.path.append(src_path)

from utils import retry_on_failure, upload_csv_to_supabase_s3
import time


@retry_on_failure(retries=3, delay=60)
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

    response = requests.get(url, params=payload)

    if response.status_code == 200:
        return response.text
    else:
        response.raise_for_status()


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
    soup = BeautifulSoup(html_content, 'html.parser')

    # Find all elements with the class 'page js-change-page'
    pages = soup.find_all('div', class_='page js-change-page')

    if pages:
        last_page = pages[-1]['data-page']
        return int(last_page)
    else:
        return None


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

    product_list = []

    for product in products:
        product_data = {}

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

    # Convert list of products to DataFrame
    product_df = pd.concat([product_df, pd.DataFrame(product_list)],
                           ignore_index=True)
    product_df = product_df.reindex(columns=product_schema.keys())

    assert list(product_df.columns) == list(
        product_schema.keys()), "DataFrame structure does not match the schema"

    return product_df


# Assume setup_logger is defined elsewhere
logger = setup_logger("logs/pingo_doce_scraper.log")

@retry_on_failure(retries=3, delay=60)
def parse_all_pages_for_category(categoria):
    """
    Fetches and parses all pages for a specific category on the Pingo Doce website.
    """
    logger.info(f"Starting to parse all pages for category: {categoria}")
    first_page_html = fetch_html_from_pingodoce(cp=1, categoria=categoria)
    last_page = parse_last_page(first_page_html)

    if last_page is None:
        last_page = 1
        logger.warning(f"Could not determine last page for category {categoria}. Assuming only 1 page.")
    else:
        logger.info(f"Found {last_page} pages for category {categoria}")

    all_products_df = pd.DataFrame()

    for cp in range(1, last_page + 1):
        logger.debug(f"Fetching page {cp} of {last_page} for category {categoria}")
        try:
            html_content = fetch_html_from_pingodoce(cp, categoria)
            products_df = parse_products_from_html(html_content)
            all_products_df = pd.concat([all_products_df, products_df], ignore_index=True)
            logger.info(f"Successfully parsed page {cp} for category {categoria}. Total products so far: {len(all_products_df)}")
        except Exception as e:
            logger.error(f"Error parsing page {cp} for category {categoria}: {str(e)}", exc_info=True)
        
        time.sleep(3)
        logger.debug(f"Waiting 3 seconds before next request")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    all_products_df["source"] = "pingo-doce"
    all_products_df["timestamp"] = timestamp

    logger.info(f"Completed parsing all pages for category {categoria}. Total products: {len(all_products_df)}")
    return all_products_df


def parse_and_save_all_categories(categories, base_path="data/raw/pingo_doce"):
    """
    Parses and saves the product data for multiple categories as CSV files.
    Also uploads the files to Supabase storage.
    """
    logger.info(f"Starting to parse and save data for {len(categories)} categories")

    base_path = base_path + "/" + datetime.now().strftime("%Y%m%d")

    if not os.path.exists(base_path):
        os.makedirs(base_path)
        logger.info(f"Created directory: {base_path}")

    supabase_folder = f"raw/pingo_doce/{datetime.now().strftime('%Y%m%d')}"

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
                upload_csv_to_supabase_s3(logger = logger, 
                                            file_path = file_path, 
                                            folder_name = supabase_folder)
            else:
                logger.warning(f"No data found for category '{categoria}'. Skipping...")
        except Exception as e:
            logger.error(f"Error processing category {categoria}: {str(e)}", exc_info=True)

    logger.info("Completed parsing and saving data for all categories")


if __name__ == "__main__":
    # Example usage
    categories = [
        "pingo-doce-lacticinios", "pingo-doce-bebidas",
        "pingo-doce-frescos-embalados", "pingo-doce-higiene-e-beleza",
        "pingo-doce-maquinas-e-capsulas-de-cafe", "pingo-doce-mercearia",
        "pingo-doce-refeicoes-prontas", "pingo-doce-cozinha-e-limpeza", "pingo-doce-congelados"
    ]

    parse_and_save_all_categories(categories)
