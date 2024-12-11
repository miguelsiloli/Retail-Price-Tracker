import pandas as pd
from datetime import datetime
import time
import numpy as np

"""
Product Name,Product ID,Price,Price per unit,Brand,Category,Image URL,Minimum Quantity,Product Link,cgid,tracking_date,source
"""

def to_unix_time(dt_str, fmt="%Y%m%d"):
    """Convert a datetime string to Unix time."""
    return int(time.mktime(datetime.strptime(str(dt_str), fmt).timetuple()))


def product_table(df):
    """
    Create a DataFrame for the PRODUCT table.

    Args:
        df (pandas.DataFrame): Input DataFrame with the original structure.

    Returns:
        pandas.DataFrame: DataFrame with columns: product_id, product_name, source.
    """
    df.rename(columns={
        'Product Name': 'product_name',
        'Product ID': 'product_id'
    }, inplace=True)

    return df[['product_id', 'product_name', 'source']] # .drop_duplicates(subset=['product_id', 'source'])

def category_table(df):
    """
    Create a DataFrame for the CATEGORY_HIERARCHY table.

    Args:
        df (pandas.DataFrame): Input DataFrame with the original structure.

    Returns:
        pandas.DataFrame: DataFrame with columns: category_level1, category_level2, category_level3.
    """
    df[['category_level1', 'category_level2', 'category_level3']] = df['Category'].str.split('/', expand=True, n=2)

    # Extract relevant columns and drop duplicates to create category hierarchy data
    df_category = df[['category_level1', 'category_level2', 'category_level3']] # .drop_duplicates()

    return df_category

def category_hierarchy_table(df):
    """
    Create a DataFrame for the category hierarchy table.

    Args:
        df (pandas.DataFrame): Input DataFrame with the original structure.

    Returns:
        pandas.DataFrame: DataFrame with columns: category_level1, category_level2, category_level3.
    """
    _len = len(df)

    # Split the category field into levels
    df['Category'] = df['Category'].fillna('') 
    df[['category_level1', 'category_level2', 'category_level3']] = df['Category'].str.split('/', expand=True, n=2)

    # Handle cases where category levels are less than 3
    df['category_level2'] = df['category_level2'].fillna(np.nan)
    df['category_level3'] = df['category_level3'].fillna(np.nan)

    assert len(df) == _len

    return df[['category_level1', 'category_level2', 'category_level3']]

# def auchan_metadata_table(df):
#     """
#     Create a DataFrame for the PRODUCT_METADATA table.

#     Args:
#         df (pandas.DataFrame): Input DataFrame with the original structure.

#     Returns:
#         pandas.DataFrame: DataFrame with columns: product_id_pk, brand, price_per_unit, minimum_quantity,
#                           product_url, product_image_url.
#     """
#     metadata_df = df[['product_id', 'product_urls', 'product_image', 'timestamp']].copy()
#     metadata_df.rename(columns={
#         'product_id': 'product_id_pk',
#         'product_urls': 'product_url',
#         'product_image': 'product_image_url'
#     }, inplace=True)
    
#     # Add columns with None values for brand, price_per_unit, and minimum_quantity
#     metadata_df['brand'] = None
#     metadata_df['price_per_unit'] = None
#     metadata_df['minimum_quantity'] = None
    
#     return metadata_df[['product_id_pk', 'brand', 'price_per_unit', 'minimum_quantity', 'product_url', 'product_image_url', 'timestamp']] # .drop_duplicates()

def product_product_pricing(df):
    """
    Create a DataFrame for the PRODUCT_CATEGORY table.

    Args:
        df (pandas.DataFrame): Input DataFrame with the original structure.

    Returns:
        pandas.DataFrame: DataFrame with columns: product_id, source, product_category, product_category2, product_category3.
    """
    df.rename(columns={
        'Price': 'product_price',
        'tracking_date': 'timestamp'
    }, inplace=True)
    price_pdf = df[['product_price', 'timestamp']]

    if not hasattr(df, 'price_currency'):
        price_pdf['price_currency'] = None

    return price_pdf

def split_price(price, precision=2):
    """
    Split the price into integer and scaled decimal parts.
    Args:
        price (float): The product price.
        precision (int): Number of decimal places to retain.
    Returns:
        tuple: (integer_part, scaled_decimal_part)
    """
    integer_part = int(price)
    decimal_part = int(round((price - integer_part) * (10**precision)))
    return integer_part, decimal_part
