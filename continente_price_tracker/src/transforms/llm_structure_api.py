import os
import pandas as pd
import json
from tqdm import tqdm

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

import re
from openai import OpenAI

class ProductNameStructurer:
    def __init__(self, openai_api_key = "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"):
        """
        Initialize the product name structurer with OpenAI API key
        
        Args:
            openai_api_key (str): OpenAI API key for authentication
        """
        # openai.api_key = openai_api_key
        self.client = OpenAI(api_key= openai_api_key) 
    
    def _extract_with_openai(self, product_names):
        """
        Use OpenAI API to extract structured product information for a chunk of product names
        
        Args:
            product_names (list): List of product names to structure
        
        Returns:
            list: List of structured product information dictionaries
        """
        # Construct prompt with all product names
        product_list = "\n".join([
            f"{i+1}. {name}" for i, name in enumerate(product_names)
        ])
        
        prompt = f"""
            Given a list of product descriptions, extract structured fields for each product. For every product, identify the following fields:

            - `weight`: Numeric value representing the weight (e.g., 170, 1, 0.5). If the weight is not explicitly stated, set it to `null`.
            - `weight_unit`: The unit of the weight (e.g., "g", "kg", "ml"). If no weight unit is present, set it to `null`.
            - `quantity`: Numeric value representing the number of items or units. If quantity is not specified, default it to `1`.
            - `qty_unit`: The unit for the quantity (e.g., "un", "duzia", "saquetas", "rolos"). Default to `"un"` if no specific unit is provided.

            For clarity, the final output must include a `product_name` field that matches the original description.

            Here are examples of how the fields should be extracted:

            1. "açúcar polegar granulado 4x170ml"
            - `weight`: 170, `weight_unit`: "ml", `quantity`: 4, `qty_unit`: "un", `product_name`: "açúcar polegar granulado 4x170ml"

            2. "açúcar polegar granulado 1 kg"
            - `weight`: 1, `weight_unit`: "kg", `quantity`: 1, `qty_unit`: "un", `product_name`: "açúcar polegar granulado 1 kg"

            3. "ovos class m meia duzia"
            - `weight`: null, `weight_unit`: null, `quantity`: 0.5, `qty_unit`: "duzia", `product_name`: "ovos class m meia duzia"

            4. "saquetas de cha 10 saquetas"
            - `weight`: null, `weight_unit`: null, `quantity`: 10, `qty_unit`: "saquetas", `product_name`: "saquetas de cha 10 saquetas"

            5. "rolos de papel 4 rolos"
            - `weight`: null, `weight_unit`: null, `quantity`: 4, `qty_unit`: "rolos", `product_name`: "rolos de papel 4 rolos"

            6. "iogurte natural 120(94)g"
            - `weight`: 120, `weight_unit`: "g", `quantity`: 1, `qty_unit`: "un", `product_name`: "iogurte natural 120(94)g"

            7. "iogurte natural 4uni"
            - `weight`: null, `weight_unit`: null, `quantity`: 4, `qty_unit`: "un", `product_name`: "iogurte natural 4uni"

            8. "iogurte 90+7 oferta"
            - `weight`: null, `weight_unit`: null, `quantity`: 90, `qty_unit`: "un", `product_name`: "iogurte 90+7 oferta"

            9. "vassoura de madeira"
            - `weight`: null, `weight_unit`: null, `quantity`: 1, `qty_unit`: "un", `product_name`: "vassoura de madeira"

            Now, process the following list of product descriptions and return a JSON array of objects with the following keys: `weight`, `weight_unit`, `quantity`, `qty_unit`, `product_name`.

            Product List:
            {product_list}

            Return the output "products" in JSON format.
            {{'weight': 33, 'weight_unit': 'cl', 'quantity': 1, 'qty_unit': 'un', 'product_name': 'Água Pingo Doce 33 cl'}}
        """

        response = self.client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {
                    "role": "system", 
                    "content": "You are an expert at extracting structured data from product names."
                },
                {
                    "role": "user", 
                    "content": prompt
                }
            ],
            temperature=0.3,
            response_format={"type": "json_object"}
        )
        aws = response.choices[0].message.content

        # Attempt to parse the response
        parsed_response = json.loads(aws)
        
        # Validate structure of the first item (assuming it's a list or dict)
        if isinstance(parsed_response, list):
            sample_item = parsed_response[0]
        elif isinstance(parsed_response, dict):
            sample_item = parsed_response
        else:
            raise ValueError("Unexpected response format")
        
        return parsed_response

    def structure_product_names(self, df, chunk_size=100, buffer_file='data/transformed/processed_products_buffer.json'):
        """
        Structure product names in the DataFrame by processing in chunks, 
        using a file-based buffer to avoid reprocessing existing product names
        
        Args:
            df (pandas.DataFrame): DataFrame with product_name column
            chunk_size (int): Number of rows to process in each chunk
            buffer_file (str): Path to the JSON file used as a buffer
        
        Returns:
            pandas.DataFrame: DataFrame with structured product information
        """
        # Create a copy of the DataFrame
        structured_df = df.copy()
        
        # Load existing buffer or create a new one
        if os.path.exists(buffer_file):
            with open(buffer_file, 'r') as f:
                buffer_data = json.load(f)
                processed_buffer = set(buffer_data.get('processed_names', []))
                processed_chunks = buffer_data.get('processed_chunks', [])
        else:
            processed_buffer = set()
            processed_chunks = []
        
        final_results = []
        
        # Process DataFrame in chunks
        for i in tqdm(range(0, len(structured_df), chunk_size)):
            # Select chunk
            chunk = structured_df.iloc[i:i+chunk_size]
            
            # Filter out already processed product names
            unprocessed_names = chunk[~chunk['product_name'].isin(processed_buffer)]['product_name'].tolist()
            
            # Skip if no unprocessed names
            # if not unprocessed_names:
            #     break

            chunk_results = self._extract_with_openai(unprocessed_names)
            # print(f"Chunk results: {chunk_results}")
            
            try:
                # case its json
                chunk_structured = pd.DataFrame(chunk_results["products"])
            except:
                # case its list
                chunk_structured = pd.DataFrame(chunk_results)
            
            # Update processed buffer with successfully processed names
            newly_processed_names = set(chunk_structured.get('product_name', []))
            processed_buffer.update(newly_processed_names)

            processed_chunks.append({
                'chunk_start_index': i,
                'processed_names': list(newly_processed_names),
                'chunk_data': chunk_structured.to_dict(orient='records')
            })
            
            # Save updated buffer to file
            with open(buffer_file, 'w') as f:
                json.dump({
                    'processed_names': list(processed_buffer),
                    'processed_chunks': processed_chunks
                }, f, indent=2)
            
            final_results.append(chunk_structured)
        
        # Combine all processed results
        if final_results:
            structured_columns = pd.concat(final_results)
            return structured_columns
        else:
            return pd.DataFrame()  # Return empty DataFrame if no results


if __name__ == "__main__":
    pingo = compile_pingo_doce_csvs()
    continente = compile_continente_csvs()
    auchan = compile_auchan_csvs()
    # print(auchan, continente)

    structurer = ProductNameStructurer()
    # result = structurer.structure_product_names(pingo)
    # result.to_csv("structured_pingo_doce.csv")

    result = structurer.structure_product_names(auchan)
    result.to_csv("data/transformed/structured_auchan.csv")

    result = structurer.structure_product_names(continente)
    result.to_csv("data/transformed/structured_continente.csv")