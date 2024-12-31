from auchan.main import preprocess_and_insert_data_auchan
from supabase_interface import ProductDatabaseInterface
from pingo_doce.main import preprocess_and_insert_data_pingo_doce
from continente.main import preprocess_and_insert_data_continente
from utils import concat_csv_from_supabase
import os
import pandas as pd

def concat_csv_files(input_path):
    # Initialize an empty list to collect DataFrames
    df_list = []
    
    # Walk through the directory and its subdirectories
    for root, dirs, files in os.walk(input_path):
        for file in files:
            # Check if the file is a CSV file
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                # Read the CSV file into a DataFrame
                try:
                    df = pd.read_csv(file_path)
                    df_list.append(df)
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")
    
    # Concatenate all the DataFrames in the list using an outer join (union)
    if df_list:
        result_df = pd.concat(df_list, ignore_index=True, sort=False, join='outer')
        return result_df
    else:
        print("No CSV files found.")
        return pd.DataFrame()  # Return an empty DataFrame if no CSV files are found



# read data from supabase buckets
# data_auchan = concat_csv_from_supabase(folder_name= "auchan")
# data_continente = concat_csv_from_supabase(folder_name= "continente")
# data_pingo_doce = concat_csv_from_supabase(folder_name= "pingo_doce")

data_auchan = concat_csv_files(input_path="raw\\auchan")
data_continente = concat_csv_files(input_path="raw\\continente")
data_pingo_doce = concat_csv_files(input_path="raw\\pingo_doce")

# Initialize the database interface
# No need to specify db_name for PostgreSQL connection
db_interface = ProductDatabaseInterface()

# Step 4: Preprocess the data and insert into the database
preprocess_and_insert_data_auchan(data_auchan, db_interface)
preprocess_and_insert_data_continente(data_continente, db_interface)
preprocess_and_insert_data_pingo_doce(data_pingo_doce, db_interface)