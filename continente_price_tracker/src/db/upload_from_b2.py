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
    print(data)
    
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

    inserted_products = connector.insert_data(standardized_data)
    return inserted_products

if __name__ == "__main__":
    # List of stores to process concurrently
    stores = ["continente", "pingo_doce", "auchan"] #,
    # current_date = datetime.now().strftime("%Y%m%d")
    # Process each store and concatenate the results
    all_products = pd.DataFrame()  # Initialize empty DataFrame to store all products
    
    for store in stores:
        print(f"Stores to process: {stores}")
        store_with_date = f"{store}" # /{current_date}
        # Get the DataFrame returned from processing the store's data
        store_products = process_data_and_generate_report(store_with_date)
        # Concatenate with the main DataFrame
        if all_products.empty:
            all_products = store_products
        else:
            all_products = pd.concat([all_products, store_products], ignore_index=True)
        
        # Define artifact path - using a shared volume that will be accessible by the next task
        artifact_dir = "./shared_data/artifacts"
        os.makedirs(artifact_dir, exist_ok=True)
        # Save the DataFrame as CSV
        artifact_path = os.path.join(artifact_dir, "inserted_products.csv") # take product_name column
        all_products.to_csv(artifact_path, index=False)
        print(f"Saved {len(all_products)} products to {artifact_path}")
