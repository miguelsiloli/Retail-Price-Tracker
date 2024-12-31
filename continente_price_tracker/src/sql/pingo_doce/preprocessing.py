import pandas as pd
from datetime import datetime
import time

"""
product_id,product_name,product_price,product_image,product_url,product_rating,source,timestamps
"""
def to_unix_time(dt_str):
    """Convert a datetime string to Unix time."""
    try:
        return int(dt_str)

    except ValueError:
        pass

    try:
        return int(time.mktime(datetime.strptime(str(dt_str), "%Y%m%d").timetuple()))
    except:
        return int(time.mktime(datetime.strptime(str(dt_str), "%Y-%m-%d").timetuple()))

def pingo_doce_product_table(df):
    """
    Create a DataFrame for the PRODUCT table.

    Args:
        df (pandas.DataFrame): Input DataFrame with the original structure.

    Returns:
        pandas.DataFrame: DataFrame with columns: product_id, product_name, source.
    """
    return df[['product_id', 'product_name', 'source']] # .drop_duplicates(subset=['product_id', 'source'])

def pingo_doce_category_table(df):
    """
    Create a DataFrame for the CATEGORY_HIERARCHY table.

    Args:
        df (pandas.DataFrame): Input DataFrame with the original structure.

    Returns:
        pandas.DataFrame: DataFrame with columns: category_level1, category_level2, category_level3.
    """
    # Extract relevant columns and drop duplicates to create category hierarchy data
    raise NotImplementedError("Pingo Doce doesn't have any categories in the data source yet.")

def pingo_doce_category_hierarchy_table(df):
    """
    Create a DataFrame for the PRODUCT_CATEGORY table.

    Args:
        df (pandas.DataFrame): Input DataFrame with the original structure.

    Returns:
        pandas.DataFrame: DataFrame with columns: product_id, source, product_category, product_category2, product_category3.
    """
    raise NotImplementedError("Pingo Doce doesn't have any categories in the data source yet.")

def pingo_doce_metadata_table(df):
    """
    Create a DataFrame for the PRODUCT_METADATA table.

    Args:
        df (pandas.DataFrame): Input DataFrame with the original structure.

    Returns:
        pandas.DataFrame: DataFrame with columns: product_id_pk, brand, price_per_unit, minimum_quantity,
                          product_url, product_image_url.
    """
    raise NotImplementedError("Pingo Doce doesn't have any metadata in the data source yet.")

def pingo_doce_product_pricing(df):
    """
    Create a DataFrame for the PRODUCT_CATEGORY table.

    Args:
        df (pandas.DataFrame): Input DataFrame with the original structure.

    Returns:
        pandas.DataFrame: DataFrame with columns: product_id, source, product_category, product_category2, product_category3.
    """
    price_pdf = df[['product_price', 'timestamp']]

    if not hasattr(df, 'price_currency'):
        price_pdf['price_currency'] = None

    return price_pdf

def convert_to_numeric(value: str) -> float:
    """
    Convert a value like '0,26€ / UN' or '3.000€ / UN' into a numeric value.
    Handles thousands separators, currency symbols, and units.
    
    :param value: The value as a string (e.g., '0,26€ / UN' or '3.000€ / UN').
    :return: The numeric value as a float.
    """
    # Remove currency symbol and unit
    value = value.replace('€', '').replace(' / UN', '').strip()
    
    # Handle thousands separators (e.g., '3.000' -> '3000')
    value = value.replace('.', '')
    
    # Convert to numeric, using ',' as the decimal separator
    value = value.replace(',', '.')
    
    try:
        # Convert the cleaned string to a float
        return float(value)
    except ValueError:
        # In case of invalid format, return NaN or handle error as needed
        return None

def split_price(price, precision=2):
    """
    Split the price into integer and scaled decimal parts.
    Args:
        price (float): The product price.
        precision (int): Number of decimal places to retain.
    Returns:
        tuple: (integer_part, scaled_decimal_part)
    """
    price = convert_to_numeric(price)
    integer_part = int(price)
    decimal_part = int(round((price - integer_part) * (10**precision)))
    return integer_part, decimal_part
