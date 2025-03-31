import pandas as pd
import os
import json
import time
import csv
from typing import List, Dict, Any

# Set up the API key
os.environ["GEMINI_API_KEY"] = "AIzaSyAuA48i-foDU79f8iauQsDRtl9zCafMZE8"

import base64
import os
from google import genai
from google.genai import types

def get_prompt_by_source(source: str) -> str:
    """
    Loads the prompt template from a text file based on the source.
    
    Args:
        source: The source of the products (continente, auchan, or pingo_doce)
        
    Returns:
        str: The prompt template for the specified source
    """
    # Validate source
    valid_sources = ["continente", "auchan", "pingo_doce"]
    if source.lower() not in valid_sources:
        raise ValueError(f"Invalid source: {source}. Must be one of {valid_sources}")
    
    # Determine the file path
    prompt_dir = "prompts"
    prompt_file = f"{prompt_dir}/{source.lower()}_prompt.txt"
    
    # Attempt to read the prompt file
    try:
        with open(prompt_file, 'r', encoding='utf-8') as file:
            prompt_template = file.read()
            return prompt_template
    except FileNotFoundError:
        raise FileNotFoundError(f"Prompt file not found: {prompt_file}")
    except Exception as e:
        raise Exception(f"Error reading prompt file {prompt_file}: {str(e)}")


def process_batch(batch: List[str], source: str) -> List[Dict[str, Any]]:
    """
    Process a batch of products and return structured JSON data
    
    Args:
        batch: List of product names to process
        source: The source of the products (continente, auchan, or pingo_doce)
        
    Returns:
        List of dictionaries with categorized product information
    """
    # Format the batch for the prompt
    product_list = "\n".join(batch)
    
    # Get the appropriate prompt based on source
    prompt_template = get_prompt_by_source(source)
    
    # Update the prompt with the product list
    prompt = prompt_template.replace("{product_list}", product_list)
    
    client = genai.Client(
        api_key=os.environ.get("GEMINI_API_KEY"),
    )

    model = "gemini-2.0-flash"
    contents = [
        types.Content(
            role="user",
            parts=[
                types.Part.from_text(text=prompt),
            ],
        )
    ]
    
    # Define schema based on source
    # Based on the prompts from the different sources, we need to adapt the schema
    if source.lower() == "continente":
        # For Continente - basic schema without brand
        schema = genai.types.Schema(
            type=genai.types.Type.ARRAY,
            description="Schema for categorized retail products from Continente",
            items=genai.types.Schema(
                type=genai.types.Type.OBJECT,
                required=["product_name", "product_type", "quantity_weight", "quantity_units", "units"],
                properties={
                    "product_name": genai.types.Schema(
                        type=genai.types.Type.STRING,
                        description="The full, original product name",
                    ),
                    "product_type": genai.types.Schema(
                        type=genai.types.Type.STRING,
                        description="The specific product category",
                    ),
                    "quantity_weight": genai.types.Schema(
                        type=genai.types.Type.NUMBER,
                        description="The numerical weight/volume value",
                    ),
                    "quantity_units": genai.types.Schema(
                        type=genai.types.Type.INTEGER,
                        description="The number of items in the package (default to 1 if not specified)",
                    ),
                    "units": genai.types.Schema(
                        type=genai.types.Type.STRING,
                        description="The unit of measurement (g, kg, ml, L, etc.)",
                    ),
                },
            ),
        )
    else:
        # For Auchan and Pingo Doce - schema with brand
        schema = genai.types.Schema(
            type=genai.types.Type.ARRAY,
            description=f"Schema for categorized retail products from {source}",
            items=genai.types.Schema(
                type=genai.types.Type.OBJECT,
                required=["product_name", "product_type", "brand", "quantity_weight", "quantity_units", "units"],
                properties={
                    "product_name": genai.types.Schema(
                        type=genai.types.Type.STRING,
                        description="The full, original product name",
                    ),
                    "product_type": genai.types.Schema(
                        type=genai.types.Type.STRING,
                        description="The specific product category",
                    ),
                    "brand": genai.types.Schema(
                        type=genai.types.Type.STRING,
                        description="The brand name or 'Generic' if no clear brand is identifiable",
                    ),
                    "quantity_weight": genai.types.Schema(
                        type=genai.types.Type.NUMBER,
                        description="The numerical weight/volume value",
                    ),
                    "quantity_units": genai.types.Schema(
                        type=genai.types.Type.INTEGER,
                        description="The number of items in the package (default to 1 if not specified)",
                    ),
                    "units": genai.types.Schema(
                        type=genai.types.Type.STRING,
                        description="The unit of measurement (g, kg, ml, L, etc.)",
                    ),
                },
            ),
        )
    
    generate_content_config = types.GenerateContentConfig(
        temperature=1,
        top_p=0.95,
        top_k=40,
        max_output_tokens=8192,
        response_mime_type="application/json",
        response_schema=schema,
    )

    full_text = ""
    for chunk in client.models.generate_content_stream(
        model=model,
        contents=contents,
        config=generate_content_config,
    ):
        full_text += chunk.text
    
    # Parse the JSON string into a Python object
    try:
        parsed_data = json.loads(full_text)
        
        # If the source is continente and we need to add a brand field for consistency
        if source.lower() == "continente":
            for item in parsed_data:
                if "brand" not in item:
                    item["brand"] = "Generic"
                    
        return parsed_data
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {e}")
        print(f"Raw response: {full_text}")
        return []


def write_to_csv(data, output_file, source=None, mode='a'):
    """
    Write data to CSV file
    
    Args:
        data: List of dictionaries with product data
        output_file: Path to the output CSV file
        source: The source of the data (continente, auchan, or pingo_doce)
        mode: File opening mode ('a' for append, 'w' for write)
    """
    file_exists = os.path.isfile(output_file) and os.path.getsize(output_file) > 0
    
    # Define fieldnames based on source if needed
    fieldnames = ['product_name', 'product_type', 'brand', 'quantity_weight', 'quantity_units', 'units']
    
    # Ensure all items have the necessary fields
    for item in data:
        for field in fieldnames:
            if field not in item:
                if field == 'brand':
                    item[field] = 'Generic'
                else:
                    item[field] = None
    
    with open(output_file, mode, newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        if not file_exists or mode == 'w':
            writer.writeheader()
        
        for item in data:
            writer.writerow(item)


def process_all(input_file = "./shared_data/artifacts/inserted_products.csv", sources = ["continente", "auchan", "pingo_doce"]):
    """
    Process products from all specified sources
    
    Args:
        input_file: Path to the CSV file containing product data
        sources: List of sources to process (continente, auchan, pingo_doce)
        
    Returns:
        DataFrame of processed results
    """
    # Read the input file
    df2 = pd.read_csv(input_file)
    
    # Drop duplicates and reset index
    df2 = df2.drop_duplicates(subset=['product_name']).reset_index(drop=True)

    # Store all results
    all_results = []

    for source in sources:
        # Filter for the current source
        df = df2[df2["source"] == source]
    
        # Get the list of products
        products = df['product_name'].tolist()
        
        # Skip if no products for this source
        if not products:
            print(f"No products found for source: {source}")
            continue
        
        # Process in batches of 100
        batch_size = 100
        total_batches = (len(products) + batch_size - 1) // batch_size
        print(f"Found {len(products)} unique products from {source} to process in {total_batches} batches")
        
        source_results = []
        
        for i in range(0, len(products), batch_size):
            batch_number = i // batch_size + 1
            end_idx = min(i + batch_size, len(products))
            batch = products[i:end_idx]
            print(f"Processing batch {batch_number}/{total_batches} ({len(batch)} products) from {source}")
            
            # Process the batch with the correct source
            results = process_batch(batch, source)
            
            if results:
                # Add source information to each result
                for item in results:
                    item['source'] = source
                
                # Add results to the source-specific list
                source_results.extend(results)
                
                # Write batch results to a source-specific CSV file
                source_output_file = f"./output/{source}_processed_products.csv"
                os.makedirs(os.path.dirname(source_output_file), exist_ok=True)
                write_to_csv(results, source_output_file, source=source)
                
                print(f"Batch {batch_number} from {source} processed and saved")
            else:
                print(f"No results obtained for batch {batch_number} from {source}")
            
            # Add delay to avoid rate limiting
            if i + batch_size < len(products):
                print("Waiting before processing next batch...")
                time.sleep(5)
        
        print(f"All batches from {source} processed. Total items: {len(source_results)}")
        
        # Add source results to the overall results
        all_results.extend(source_results)
    
    # Convert all results to DataFrame and write to a combined CSV
    if all_results:
        results_df = pd.DataFrame(all_results)
        combined_output_file = "./output/all_processed_products.csv"
        results_df.to_csv(combined_output_file, index=False)
        print(f"All results combined and saved to {combined_output_file}")
    else:
        results_df = pd.DataFrame()
        print("No results to save")
    
    # Return the combined results as a DataFrame
    return results_df