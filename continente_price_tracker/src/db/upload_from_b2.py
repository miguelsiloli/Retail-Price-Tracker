import concurrent.futures
from ingestion.utils import concat_csv_from_supabase
from ingestion.preprocessing import ProductDataStandardizer
from typing import Dict, Union, Optional
from io import StringIO
import pandas as pd
import re
from ingestion.db_connector import PostgresConnector
from ingestion.transforms import expand_product_fields
from ingestion.utils import concat_csv_from_b2
from datetime import datetime
import os

connector = PostgresConnector()

# Define a function to load and process data for each store
def process_data_and_generate_report(folder_name):
    # Load the raw data
    data = concat_csv_from_b2(folder_name=folder_name)
    
    # Standardize the data
    std = ProductDataStandardizer()
    standardized_data = std.standardize_data(data, folder_name)
    
    # Create a mapping of unique product names to their expanded fields
    unique_products = standardized_data['product_name'].unique()
    unique_products_df = pd.DataFrame({'product_name': unique_products})
    
    # Use apply to expand product fields and collect the results in a list
    expanded_rows = unique_products_df['product_name'].apply(lambda row: expand_product_fields(row)).tolist()
    
    # Concatenate all DataFrames in the list into a single DataFrame
    product_lookup = pd.concat(expanded_rows, ignore_index=True)

    standardized_data = pd.merge(
        standardized_data, 
        product_lookup, 
        how='left', 
        on='product_name'
    )

    connector.insert_data(standardized_data)
    return standardized_data
