from auchan.main import preprocess_and_insert_data_auchan
from supabase_interface import ProductDatabaseInterface
from pingo_doce.main import preprocess_and_insert_data_pingo_doce
from continente.main import preprocess_and_insert_data_continente
from utils import concat_csv_from_supabase

data_auchan = concat_csv_from_supabase("auchan")
data_continente = concat_csv_from_supabase("continente")
data_pingo_doce = concat_csv_from_supabase("pingo_doce")

# Initialize the database interface
# No need to specify db_name for PostgreSQL connection
db_interface = ProductDatabaseInterface()

# Path to the Parquet file
parquet_file = 'data/raw/auchan_combined.parquet'
continente_file = 'data/raw/continente.parquet'
pingo_doce_file = 'data/raw/pingo_doce.parquet'

# Step 4: Preprocess the data and insert into the database
preprocess_and_insert_data_auchan(data_auchan, db_interface)
preprocess_and_insert_data_continente(data_continente, db_interface)
preprocess_and_insert_data_pingo_doce(data_pingo_doce, db_interface)