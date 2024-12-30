"""
Pingo Doce: Kitty, Go Active, Pingo Doce Biológico, Pingo Doce, Skino, ActivePet, Pura Vida, Be Beauty, Ultra, Cuida Bebe, Iguarias Pingo Doce
Auchan: Qilive, Cosmia, Polegar, Actuel, Auchan Baby, Auchan à mesa, Auchan Collection, Auchan Bio, Auchan Very Veggie, Airport, Gardenstar, One Two Fun  
"""

import os
import pandas as pd
import json
from tqdm import tqdm
import re

def compile_pingo_doce_csvs(base_path='data\\raw\\pingo_doce'):
    """
    Read all CSV files in subfolders of the specified base path
    and compile them into a single DataFrame with unique product_id and product_name.
    
    Args:
        base_path (str): Path to the root directory containing CSV files
    
    Returns:
        pandas.DataFrame: Compiled DataFrame with unique product_id and product_name
    """
    # List to store individual DataFrames
    dataframes = []
    
    # Walk through all subdirectories
    for root, dirs, files in os.walk(base_path):
        for file in files:
            # Check if file is a CSV
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                try:
                    # Read CSV file
                    df = pd.read_csv(file_path)
                    
                    # Ensure the DataFrame has product_id and product_name columns
                    if 'product_id' in df.columns and 'product_name' in df.columns:
                        dataframes.append(df[['product_id', 'product_name']])
                except Exception as e:
                    print(f"Error reading file {file_path}: {e}")
    
    # Combine all DataFrames and drop duplicates
    if dataframes:
        combined_df = pd.concat(dataframes, ignore_index=True)
        unique_products = combined_df.drop_duplicates(subset=['product_id', 'product_name'])
        return unique_products
    else:
        print("No CSV files found in the specified path.")
        return pd.DataFrame(columns=['product_id', 'product_name'])

def compile_auchan_csvs(base_path='data\\raw\\auchan'):
    """
    Read all CSV files in subfolders of the specified base path
    and compile them into a single DataFrame with unique product_id and product_name.
    
    Args:
        base_path (str): Path to the root directory containing CSV files
    
    Returns:
        pandas.DataFrame: Compiled DataFrame with unique product_id and product_name
    """
    # List to store individual DataFrames
    dataframes = []
    
    # Walk through all subdirectories
    for root, dirs, files in os.walk(base_path):
        for file in files:
            # Check if file is a CSV
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                try:
                    # Read CSV file
                    df = pd.read_csv(file_path)
                    
                    # Ensure the DataFrame has product_id and product_name columns
                    if 'product_id' in df.columns and 'product_name' in df.columns:
                        dataframes.append(df[['product_id', 'product_name']])
                except Exception as e:
                    print(f"Error reading file {file_path}: {e}")
    
    # Combine all DataFrames and drop duplicates
    if dataframes:
        combined_df = pd.concat(dataframes, ignore_index=True)
        unique_products = combined_df.drop_duplicates(subset=['product_id', 'product_name'])
        return unique_products
    else:
        print("No CSV files found in the specified path.")
        return pd.DataFrame(columns=['product_id', 'product_name'])


def compile_continente_csvs(base_path='data\\raw\\continente'):
    """
    Read all CSV files in subfolders of the specified base path
    and compile them into a single DataFrame with unique product_id and product_name.
    
    Args:
        base_path (str): Path to the root directory containing CSV files
    
    Returns:
        pandas.DataFrame: Compiled DataFrame with unique product_id and product_name
    """
    # List to store individual DataFrames
    dataframes = []
    
    # Walk through all subdirectories
    for root, dirs, files in os.walk(base_path):
        for file in files:
            # Check if file is a CSV
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                try:
                    # Read CSV file
                    df = pd.read_csv(file_path)
                    
                    # Ensure the DataFrame has product_id and product_name columns
                    if 'Product ID' in df.columns and 'Product Name' in df.columns:
                        dataframes.append(df[['Product ID', 'Product Name']])
                except Exception as e:
                    print(f"Error reading file {file_path}: {e}")
    
    # Combine all DataFrames and drop duplicates
    if dataframes:
        combined_df = pd.concat(dataframes, ignore_index=True)
        unique_products = combined_df.drop_duplicates(subset=['Product ID', 'Product Name'])
        unique_products.rename(columns={'Product Name': 'product_name', 'Product ID': 'product_id'}, inplace=True)
        return unique_products
    else:
        print("No CSV files found in the specified path.")
        return pd.DataFrame(columns=['product_id', 'product_name'])


pingo = compile_pingo_doce_csvs()
continente = compile_continente_csvs()
auchan = compile_auchan_csvs()

def extract_brand(product_name, retailer_brands):
    """
    Extract brand name from product name using retailer-specific brands
    
    Parameters:
    -----------
    product_name : str
        The full product name to extract brand from
    retailer_brands : dict
        Dictionary of brands for each retailer
    
    Returns:
    --------
    str or None
        Extracted brand name or None if no match found
    """
    # Normalize the product name (convert to lowercase, remove extra spaces)
    normalized_name = product_name.lower().strip()
    
    # Create regex patterns for each brand in the retailer's brand list
    for brand in retailer_brands:
        # Create a case-insensitive regex pattern
        # Use word boundaries to prevent partial matches
        pattern = r'\b' + re.escape(brand.lower()) + r'\b'
        
        # Check if the brand is in the product name
        if re.search(pattern, normalized_name):
            return brand
    
    return None

# Define brand lists for retailers
retailer_brands = {
    'Pingo Doce': [
        'Kitty', 'Go Active', 'Pingo Doce Biológico', 'Pingo Doce', 
        'Skino', 'ActivePet', 'Pura Vida', 'Be Beauty', 'Ultra', 
        'Cuida Bebe', 'Iguarias Pingo Doce'
    ],
    'Auchan': [
        'Qilive', 'Cosmia', 'Polegar', 'Actuel', 'Auchan Baby', 
        'Auchan à mesa', 'Auchan Collection', 'Auchan Bio', 
        'Auchan Very Veggie', 'Airport', 'Gardenstar', 'One Two Fun'
    ]
}

# Example usage function
def process_retailer_brands(df, retailer_name):
    """
    Process brands for a specific retailer's dataframe
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame containing product names
    retailer_name : str
        Name of the retailer to process
    
    Returns:
    --------
    pandas.DataFrame
        DataFrame with added 'brand' column
    """
    # Create a copy of the dataframe to avoid modifying the original
    processed_df = df.copy()
    
    # Extract brands
    processed_df['brand'] = processed_df['product_name'].apply(
        lambda x: extract_brand(x, retailer_brands.get(retailer_name, []))
    )
    
    return processed_df

pingo_processed = process_retailer_brands(pingo, 'Pingo Doce')
auchan_processed = process_retailer_brands(auchan, 'Auchan')

# Print results
print("Pingo Doce Brands:")
print(pingo_processed)
print("\nAuchan Brands:")
print(auchan_processed)