import psycopg2
from tqdm import tqdm
from psycopg2.extras import execute_batch, execute_values
from auchan.preprocessing import split_price, to_unix_time
import logging
from dotenv import load_dotenv
import os
import hashlib

load_dotenv()

SUPABASE_DB_CREDENTIALS = {
    "DATABASE_NAME": os.getenv("DATABASE_NAME"),
    "USER": os.getenv("USER"),
    "PASSWORD": os.getenv("PASSWORD"),
    "HOST": os.getenv("HOST")
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('database_operations.log'),
        logging.StreamHandler()
    ]
)

class ProductDatabaseInterface:
    """
    A class to interface with the PostgreSQL product database.

    This class provides methods to insert data into the PRODUCT, PRODUCT_PRICING,
    CATEGORY_HIERARCHY, PRODUCT_CATEGORY tables of the database.
    """

    def __init__(self):
        """
        Initialize the ProductDatabaseInterface with PostgreSQL connection.
        Uses environment variables for connection details.
        """
        self.conn = psycopg2.connect(
            dbname=SUPABASE_DB_CREDENTIALS.get("DATABASE_NAME"),
            user=SUPABASE_DB_CREDENTIALS.get("USER"),
            password=SUPABASE_DB_CREDENTIALS.get("PASSWORD"),
            host=SUPABASE_DB_CREDENTIALS.get("HOST")
        )
        self.cursor = self.conn.cursor()

    def text_to_integer_encoding(self, text: str) -> int:
        text = str(text)
        # Use SHA-256 to ensure deterministic output
        # Take the first 8 bytes of the hash and convert it to an integer
        hash_bytes = hashlib.sha256(text.encode('utf-8')).digest()[:8]  # Get the first 8 bytes
        return int.from_bytes(hash_bytes, byteorder='big', signed=True)

    def insert_into_product_table(self, df_product):
        """
        Insert product data into the PRODUCT table and return the product_id_pk.
        """
        logging.info(f"Starting insert_into_product_table with {len(df_product)} records")
        product_ids_pk = []

        for _, row in tqdm(df_product.iterrows(), total=len(df_product), desc="Inserting into PRODUCT"):
            insert_query = '''
                INSERT INTO PRODUCT (product_id, product_name, source)
                VALUES (%s, %s, %s)
                ON CONFLICT (product_id, source) DO NOTHING
                RETURNING product_id_pk
            '''
            self.cursor.execute(insert_query, (
                row["product_id"], 
                row["product_name"], 
                row["source"]
            ))
            
            select_query = '''
                SELECT product_id_pk FROM PRODUCT 
                WHERE product_id = %s AND source = %s
            '''
            self.cursor.execute(select_query, (row["product_id"], row["source"]))
            product_id_pk = self.cursor.fetchone()[0]
            product_ids_pk.append(product_id_pk)

        self.conn.commit()
        logging.info(f"Completed insert_into_product_table. Inserted {len(product_ids_pk)} records")
        return product_ids_pk

    def insert_into_product_pricing_table(self, df_pricing, product_ids_pk):
        """
        Insert pricing data into the PRODUCT_PRICING table with split price.
        """
        logging.info(f"Starting insert_into_product_pricing_table with {len(df_pricing)} records")
        
        insert_query = '''
            INSERT INTO PRODUCT_PRICING 
            (product_id_pk, price_integer, price_decimal, price_currency, timestamp)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (product_id_pk, timestamp) DO NOTHING
        '''

        for i, (_, row) in enumerate(tqdm(df_pricing.iterrows(), total=len(df_pricing), desc="Inserting into PRODUCT_PRICING")):
            price_integer, price_decimal = split_price(row[0])
            unix_timestamp = to_unix_time(row[1])
            
            self.cursor.execute(insert_query, (
                product_ids_pk[i], 
                price_integer, 
                price_decimal, 
                row[2], 
                unix_timestamp
            ))
        
        self.conn.commit()
        logging.info(f"Completed insert_into_product_pricing_table")

    def insert_into_category_hierarchy_table(self, df_category):
        """
        Insert category hierarchy data into the CATEGORY_HIERARCHY table.
        """
        logging.info(f"Starting insert_into_category_hierarchy_table with {len(df_category)} records")
        category_ids = []

        for _, row in tqdm(df_category.iterrows(), total=len(df_category), desc="Inserting into CATEGORY_HIERARCHY"):
            insert_query = '''
                INSERT INTO CATEGORY_HIERARCHY 
                (category_level1, category_level2, category_level3)
                VALUES (%s, %s, %s)
                ON CONFLICT (category_level1, category_level2, category_level3) DO NOTHING
                RETURNING category_id
            '''
            self.cursor.execute(insert_query, (
                row["category_level1"], 
                row["category_level2"], 
                row["category_level3"]
            ))
            
            select_query = '''
                SELECT category_id FROM CATEGORY_HIERARCHY
                WHERE (category_level1 = %s OR category_level1 IS NULL)
                AND (category_level2 = %s OR category_level2 IS NULL)
                AND (category_level3 = %s OR category_level3 IS NULL)
                LIMIT 1
            '''
            self.cursor.execute(select_query, (
                row["category_level1"], 
                row["category_level2"], 
                row["category_level3"]
            ))
            category_ids.append(self.cursor.fetchone()[0])

        self.conn.commit()
        logging.info(f"Completed insert_into_category_hierarchy_table. Inserted {len(category_ids)} records")
        return category_ids

    def insert_into_product_category_table(self, df_product_category, category_ids, product_ids_pk):
        """
        Insert product-category mappings into the PRODUCT_CATEGORY table.
        """
        logging.info(f"Starting insert_into_product_category_table with {len(df_product_category)} records")
        
        insert_query = '''
            INSERT INTO PRODUCT_CATEGORY (product_id_pk, category_id)
            VALUES (%s, %s)
            ON CONFLICT (product_id_pk, category_id) DO NOTHING
        '''

        for idx, row in enumerate(tqdm(df_product_category.itertuples(index=False), total=len(df_product_category), desc="Inserting into PRODUCT_CATEGORY")):
            self.cursor.execute(insert_query, (
                product_ids_pk[idx], 
                category_ids[idx]
            ))

        self.conn.commit()
        logging.info("Completed insert_into_product_category_table")

    def bulk_insert_into_product_table(self, df_product, chunk_size=10000):
        """
        Bulk insert products using execute_values.
        """
        logging.info(f"Starting bulk_insert_into_product_table with {len(df_product)} records")
        
        df_product['product_id'] = df_product['product_id'].apply(self.text_to_integer_encoding)
        logging.info(f"Number of product IDs after encoding: {len(df_product['product_id'])}")

        data_to_insert = df_product[['product_id', 'product_name', 'source']].values.tolist()
        product_ids_pk = []

        for i in tqdm(range(0, len(data_to_insert), chunk_size), desc="Inserting Products"):
            chunk = data_to_insert[i:i + chunk_size]
        
            insert_query = '''
                INSERT INTO PRODUCT (product_id, product_name, source)
                VALUES %s
                ON CONFLICT (product_id, source) DO NOTHING
            '''
            execute_values(self.cursor, insert_query, chunk)
        
            select_query = '''
                SELECT product_id, product_id_pk
                FROM PRODUCT
                WHERE (product_id, source) IN (
                    SELECT unnest(%s), unnest(%s)
                )
            '''
        
            product_ids = [row[0] for row in chunk]
            sources = [row[2] for row in chunk]
        
            self.cursor.execute(select_query, (product_ids, sources))
            returned_records = self.cursor.fetchall()
            id_mapping = {record[0]: record[1] for record in returned_records}
            chunk_product_ids_pk = [id_mapping[row[0]] for row in chunk]
            product_ids_pk.extend(chunk_product_ids_pk)
    
        logging.info(f"Number of product IDs after insertion: {len(product_ids_pk)}")
        self.conn.commit()
        logging.info("Completed bulk_insert_into_product_table")
        return product_ids_pk

    def bulk_insert_into_product_pricing_table(self, df_pricing, product_ids_pk):
        """
        Bulk insert product pricing using execute_values with prefiltering of existing records.
        """
        logging.info(f"Starting bulk_insert_into_product_pricing_table with {len(df_pricing)} records")
        
        # Prepare initial data
        data_to_insert = []
        for i, (_, row) in tqdm(enumerate(df_pricing.iterrows()), total=len(df_pricing), desc="Preparing Pricing Data"):
            price_integer, price_decimal = split_price(row[0])
            unix_timestamp = to_unix_time(row[1])
            
            data_to_insert.append((
                product_ids_pk[i],
                price_integer,
                price_decimal,
                row[2],
                unix_timestamp
            ))
        
        # Remove duplicates from data_to_insert based on product_id and timestamp
        seen = set()
        unique_data = []
        duplicate_count = 0
        
        for record in data_to_insert:
            key = (record[0], record[4])  # (product_id_pk, timestamp)
            if key not in seen:
                seen.add(key)
                unique_data.append(record)
            else:
                duplicate_count += 1
        
        # Query existing records
        existing_query = '''
            SELECT product_id_pk, timestamp 
            FROM PRODUCT_PRICING 
            WHERE (product_id_pk, timestamp) = ANY(%s)
        '''
        # Create list of (product_id, timestamp) tuples for the query
        keys_to_check = [(record[0], record[4]) for record in unique_data]
        self.cursor.execute(existing_query, (keys_to_check,))
        
        # Create set of existing composite keys
        existing_keys = {(row[0], row[1]) for row in self.cursor.fetchall()}
        
        # Filter out existing records
        filtered_data = [
            record for record in unique_data 
            if (record[0], record[4]) not in existing_keys
        ]
        
        if filtered_data:
            logging.info(
                f"Found {duplicate_count} duplicates in input data. "
                f"Filtered out {len(existing_keys)} existing records. "
                f"Inserting {len(filtered_data)} new records."
            )
            
            insert_query = '''
                INSERT INTO PRODUCT_PRICING
                (product_id_pk, price_integer, price_decimal, price_currency, timestamp)
                VALUES (%s, %s, %s, %s, %s)
            '''
            
            execute_batch(self.cursor, insert_query, filtered_data, page_size=1000)
            self.conn.commit()
        else:
            logging.info(
                f"Found {duplicate_count} duplicates in input data. "
                f"All remaining {len(existing_keys)} records already exist in database. "
                "No new records to insert."
            )
        
        logging.info("Completed bulk_insert_into_product_pricing_table")

    def bulk_insert_into_category_hierarchy_table_with_defaults(self, df_category):
        """
        Bulk insert category hierarchy using execute_values.
        """
        logging.info(f"Starting bulk_insert_into_category_hierarchy_table_with_defaults with {len(df_category)} records")
        
        data_to_insert = df_category[['category_level1', 'category_level2', 'category_level3']].fillna('-1').values.tolist()
        category_ids = []
        chunk_size = 1000

        default_category = ['-1', '-1', '-1']
        
        self.cursor.execute('''
            INSERT INTO CATEGORY_HIERARCHY (category_level1, category_level2, category_level3)
            VALUES (%s, %s, %s)
            ON CONFLICT (category_level1, category_level2, category_level3) DO NOTHING
        ''', default_category)

        self.cursor.execute('''
            SELECT category_id 
            FROM CATEGORY_HIERARCHY
            WHERE category_level1 = %s AND category_level2 = %s AND category_level3 = %s
        ''', default_category)
        default_category_id = self.cursor.fetchone()[0]
        
        for i in tqdm(range(0, len(data_to_insert), chunk_size), desc="Inserting Categories"):
            chunk = data_to_insert[i:i + chunk_size]

            insert_query = '''
                INSERT INTO CATEGORY_HIERARCHY
                (category_level1, category_level2, category_level3)
                VALUES %s
                ON CONFLICT (category_level1, category_level2, category_level3) DO NOTHING
            '''
            execute_values(self.cursor, insert_query, chunk)

            select_query = '''
                SELECT 
                    category_level1, 
                    category_level2, 
                    category_level3, 
                    category_id
                FROM CATEGORY_HIERARCHY
                WHERE (category_level1, category_level2, category_level3) IN (
                    SELECT unnest(%s), unnest(%s), unnest(%s)
                )
            '''

            level1 = [row[0] for row in chunk]
            level2 = [row[1] for row in chunk]
            level3 = [row[2] for row in chunk]

            self.cursor.execute(select_query, (level1, level2, level3))
            category_mapping = {(row[0], row[1], row[2]): row[3] for row in self.cursor.fetchall()}
            chunk_ids = [
                category_mapping.get((row[0], row[1], row[2]), default_category_id) 
                for row in chunk
            ]
            category_ids.extend(chunk_ids)

        self.conn.commit()
        logging.info(f"Completed bulk_insert_into_category_hierarchy_table_with_defaults. Inserted {len(category_ids)} records")
        return category_ids

    def bulk_insert_into_product_category_table(self, df_product_category, product_ids_pk, category_ids):
        """
        Bulk insert product-category relationships using execute_batch with prefiltering of existing records.
        """
        logging.info(f"Starting bulk_insert_into_product_category_table with {len(df_product_category)} records")
        
        # Create set of product_id_pk pairs to insert
        data_to_insert = set()
        for idx in tqdm(range(len(df_product_category)), desc="Preparing Product-Category Relationships"):
            product_id = product_ids_pk[idx]
            category_id = category_ids[idx] if category_ids[idx] is not None else -1
            data_to_insert.add((product_id, category_id))
        
        # Query existing product_id_pk values
        existing_query = '''
            SELECT product_id_pk 
            FROM PRODUCT_CATEGORY 
        '''
        self.cursor.execute(existing_query, ([pid for pid, _ in data_to_insert],))
        existing_product_ids = {row[0] for row in self.cursor.fetchall()}
        
        # Remove existing records from data_to_insert
        filtered_data = [
            (pid, cid) for pid, cid in data_to_insert 
            if pid not in existing_product_ids
        ]
        
        if filtered_data:
            logging.info(f"Inserting {len(filtered_data)} new records after filtering out {len(existing_product_ids)} existing records")
            
            insert_query = '''
                INSERT INTO PRODUCT_CATEGORY (product_id_pk, category_id)
                VALUES (%s, %s)
            '''
            
            execute_batch(self.cursor, insert_query, filtered_data, page_size=100)
            self.conn.commit()
        else:
            logging.info("No new records to insert after filtering")
        
        logging.info("Completed bulk_insert_into_product_category_table")

    def __del__(self):
        """
        Close database connections when object is deleted.
        """
        if hasattr(self, 'cursor'):
            self.cursor.close()
        if hasattr(self, 'conn'):
            self.conn.close()