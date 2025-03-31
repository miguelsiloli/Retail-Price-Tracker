import pandas as pd
import glob

# Function to read and concatenate all CSV files in a given path
def read_and_concat_csvs(directory_path):
    csv_files = glob.glob(f"{directory_path}/*.csv")
    dataframes = [pd.read_csv(file) for file in csv_files]
    concatenated_df = pd.concat(dataframes, ignore_index=True)
    return concatenated_df

# Reading and concatenating CSV files for each directory
continente_df = read_and_concat_csvs(r"data/raw/continente/20241113")
pingo_doce_df = read_and_concat_csvs(r"data/raw/pingo_doce/20241113")
auchan_df = read_and_concat_csvs(r"data/raw/auchan/20241113")

# continente_df["Product Name"] = continente_df["Product Name"] + continente_df["Brand"]

# Display the results (optional)
print(len(continente_df))
print(len(pingo_doce_df))
print(len(auchan_df))
# print(continente_df.columns)
# print(pingo_doce_df.columns)
# print(auchan_df.columns)

# Rename columns in both DataFrames
continente_df = continente_df.rename(columns={
    'Product Name': 'product_name',
    'Price per unit': 'price',
})[["product_name", "price"]]

pingo_doce_df = pingo_doce_df.rename(columns={
    'product_name': 'product_name',
    'product_price': 'price'
})[["product_name", "price"]]

auchan_df = auchan_df.rename(columns={
    'product_name': 'product_name',
    'product_price': 'price'
})[["product_name", "price"]]

# Concatenate the DataFrames vertically
merged_df = pd.concat([continente_df, pingo_doce_df, auchan_df], axis=0, ignore_index=True)

# Add a new column to identify the source
merged_df['source'] = ['Continente'] * len(continente_df) + ['Pingo Doce'] * len(pingo_doce_df) + ["Auchan"] * len(auchan_df)

# Function to clean and convert price to float
def process_price(price):
    price = str(price)
    try:
        # Remove currency symbol and units
        clean_price = price.replace('€', '').split('/')[0].strip()
        
        # Check if the price has a European format (comma as decimal separator)
        if ',' in clean_price and '.' in clean_price:
            # Remove thousands separator (.) and replace comma with dot for decimals
            clean_price = clean_price.replace('.', '').replace(',', '.')
        elif ',' in clean_price:
            # Only replace comma with dot if no thousands separator is present
            clean_price = clean_price.replace(',', '.')
        elif '.' in clean_price:
            # If only dot is present, assume it's a standard decimal
            clean_price = clean_price
        
        # Convert to float
        return float(clean_price)
    except ValueError:
        return None

# Apply the function to the price column
merged_df['price'] = merged_df['price'].apply(process_price).dropna()
merged_df.to_csv("price_data.csv")

# Display the results
print(merged_df["price"].describe())