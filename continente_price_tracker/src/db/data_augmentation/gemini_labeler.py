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
    # Determine the file path
    prompt_dir = "prompts"
    prompt_file = f"{prompt_dir}/{source.lower()}_prompt.txt"
    
    # Create the prompts directory if it doesn't exist
    # os.makedirs(prompt_dir, exist_ok=True)
    
    with open(prompt_file, 'r', encoding='utf-8') as file:
        return file.read()


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
    
    client = genai.Client(
        api_key=os.environ.get("GEMINI_API_KEY"),
    )

    model = "gemini-2.0-flash"
    contents = [
        types.Content(
            role="user",
            parts=[
                types.Part.from_text(text=prompt_template),
            ],
        )
    ]
    
    generate_content_config = types.GenerateContentConfig(
        temperature=1,
        top_p=0.95,
        top_k=40,
        max_output_tokens=8192,
        response_mime_type="application/json",
        response_schema=genai.types.Schema(
            type = genai.types.Type.ARRAY,
            description = "Schema for categorized retail products",
            items = genai.types.Schema(
                type = genai.types.Type.OBJECT,
                required = ["product_name", "product_type", "brand", "quantity_weight", "quantity_units", "units"],
                properties = {
                    "product_name": genai.types.Schema(
                        type = genai.types.Type.STRING,
                        description = "The full, original product name",
                    ),
                    "product_type": genai.types.Schema(
                        type = genai.types.Type.STRING,
                        description = "The specific product category",
                    ),
                    "brand": genai.types.Schema(
                        type = genai.types.Type.STRING,
                        description = "The brand name or 'Generic' if no clear brand is identifiable",
                    ),
                    "quantity_weight": genai.types.Schema(
                        type = genai.types.Type.NUMBER,
                        description = "The numerical weight/volume value",
                    ),
                    "quantity_units": genai.types.Schema(
                        type = genai.types.Type.INTEGER,
                        description = "The number of items in the package (default to 1 if not specified)",
                    ),
                    "units": genai.types.Schema(
                        type = genai.types.Type.STRING,
                        description = "The unit of measurement (g, kg, ml, L, etc.)",
                    ),
                },
            ),
        ),
    )

    full_text = ""
    for chunk in client.models.generate_content_stream(
        model=model,
        contents=contents,
        config=generate_content_config,
    ):
        full_text += chunk.text
    
    # Parse the JSON string into a Python object
    parsed_data = json.loads(full_text)
    return parsed_data

def write_to_csv(data, output_file, mode='a'):
    """Write data to CSV file"""
    file_exists = os.path.isfile(output_file) and os.path.getsize(output_file) > 0
    
    with open(output_file, mode, newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['product_name', 'product_type', 'brand', 'quantity_weight', 'quantity_units', 'units'])
        
        if not file_exists or mode == 'w':
            writer.writeheader()
        
        for item in data:
            writer.writerow(item)

def process_all(input_file = "./shared_data/artifacts/inserted_products.csv", sources = ["continente", "auchan", "pingo_doce"]):
    # Read the input file
    df2 = pd.read_csv(input_file)
    
    # Drop duplicates and reset index
    df2 = df2.drop_duplicates(subset=['product_name']).reset_index(drop=True)

    # Store all results
    all_results = []

    for source in sources:
        df = df2[df2["source"] == source]
    
        # Get the list of products
        products = df['product_name'].tolist()
        
        # Process in batches of 100
        batch_size = 100
        total_batches = (len(products) + batch_size - 1) // batch_size
        print(f"Found {len(products)} unique products to process in {total_batches} batches")
        
        for i in range(0, len(products), batch_size):
            batch_number = i // batch_size + 1
            end_idx = min(i + batch_size, len(products))
            batch = products[i:end_idx]
            print(f"Processing batch {batch_number}/{total_batches} ({len(batch)} products)")
            
            # Process the batch
            results = process_batch(batch)
            
            if results:
                # Add results to the combined list
                all_results.extend(results)
                print(f"Batch {batch_number} processed and saved")
            else:
                print(f"No results obtained for batch {batch_number}")
            
            # Add delay to avoid rate limiting
            if i + batch_size < len(products):
                print("Waiting before processing next batch...")
                time.sleep(5)
        
        print(f"All batches processed.")
    
    # Convert all results to DataFrame
    results_df = pd.DataFrame(all_results)
    return results_df