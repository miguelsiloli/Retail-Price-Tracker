import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from typing import Optional
import hashlib
import logging
from datetime import datetime
from dotenv import load_dotenv
import os
import uuid
import numpy as np

load_dotenv()

# Accessing the variables
database_name = os.getenv("DATABASE_NAME")
user = os.getenv("USER")
password = os.getenv("PASSWORD")
host = os.getenv("HOST")


class PostgresConnector:
    def __init__(
        self,
        host: str = host,
        database: str = database_name,
        user: str = user,
        password: str = password,
        port: int = 5432,
        batch_size: int = 5000
    ):
        """Initialize PostgreSQL connector with connection parameters."""
        self.connection_params = {
            "host": host,
            "database": database,
            "user": user,
            "password": password,
            "port": port
        }
        self.batch_size = batch_size
        self.logger = logging.getLogger(__name__)

    def connect(self) -> psycopg2.extensions.connection:
        """Create and return a database connection."""
        # Force TCP connection by explicitly setting sslmode
        connection_params = self.connection_params.copy()
        connection_params['sslmode'] = 'prefer'
        
        # Print connection info for debugging (remove sensitive info in production)
        print(f"Connecting to: {connection_params['host']}:{connection_params['port']} as {connection_params['user']}")
        
        return psycopg2.connect(**connection_params)

    def _convert_to_list(self, df: pd.DataFrame, columns: list) -> list:
        """Convert DataFrame columns to list of tuples, handling NULL values."""
        return df[columns].replace({np.nan: None}).values.tolist()

    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and prepare DataFrame for insertion."""
        # Create a copy to avoid modifying the original
        df = df.copy()
        
        # Replace empty strings with None
        df = df.replace(r'^\s*$', None, regex=True)
        
        # Convert price to float, handling any invalid values
        df['product_price'] = pd.to_numeric(df['product_price'], errors='coerce')
        
        # Convert timestamp to Unix timestamp (bigint)
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df['timestamp'] = df['timestamp'].astype('int64') // 10**9  # Convert to Unix timestamp
        
        return df

    def generate_product_id_pk(self, product_id: str, source: str) -> int:
        """Generate a unique product ID primary key as a positive integer."""
        if pd.isna(product_id) or pd.isna(source):
            return None
        combined = f"{str(product_id)}_{str(source)}"
        # Take only first 8 chars (32 bits) to fit in PostgreSQL integer
        hex_hash = hashlib.md5(combined.encode()).hexdigest()[:8]
        return int(hex_hash, 16) & 0x7FFFFFFF  # Ensure positive 32-bit integer

    def generate_category_id(self, level1: str, level2: str, level3: str) -> int:
        """Generate a unique category ID as a positive integer."""
        # Replace None/NaN with empty string to maintain consistency
        level1 = str(level1) if not pd.isna(level1) else ""
        level2 = str(level2) if not pd.isna(level2) else ""
        level3 = str(level3) if not pd.isna(level3) else ""
        combined = f"{level1}_{level2}_{level3}"
        # Take only first 8 chars (32 bits) to fit in PostgreSQL integer
        hex_hash = hashlib.md5(combined.encode()).hexdigest()[:8]
        return int(hex_hash, 16) & 0x7FFFFFFF  # Ensure positive 32-bit integer

    def insert_data(self, df: pd.DataFrame) -> None:
        """
        Insert data from DataFrame into PostgreSQL tables.
        
        Args:
            df: DataFrame with columns [product_id, product_name, product_price, 
                category_level1, category_level2, category_level3, timestamp, 
                source, quantity, qty_unit, weight, weight_unit]
        """
        if df.empty:
            self.logger.warning("Empty DataFrame provided, skipping insertion")
            return

        # Prepare the data
        df['product_id_pk'] = df.apply(
            lambda x: self.generate_product_id_pk(x['product_id'], x['source']), 
            axis=1
        )
        
        df['category_id'] = df.apply(
            lambda x: self.generate_category_id(
                x['category_level1'], 
                x['category_level2'], 
                x['category_level3']
            ), 
            axis=1
        )

        # Split price into integer and decimal parts
        df['price_integer'] = df['product_price'].astype(float).astype(int)
        df['price_decimal'] = ((df['product_price'].astype(float) % 1) * 100).astype(int)
        df['price_currency'] = 'EUR'  # Assuming EUR as default currency

        with self.connect() as conn:
            with conn.cursor() as cur:
                try:
                    # Insert category hierarchy
                    self._insert_category_hierarchy(cur, df)
                    
                    # Insert products
                    self._insert_products(cur, df)
                    
                    # Insert product categories
                    self._insert_product_categories(cur, df)
                    
                    # Insert product pricing
                    self._insert_product_pricing(cur, df)
                    
                    conn.commit()
                    self.logger.info(f"Successfully inserted {len(df)} records")
                    
                except Exception as e:
                    conn.rollback()
                    self.logger.error(f"Error inserting data: {str(e)}")
                    raise

    def _insert_category_hierarchy(self, cur: psycopg2.extensions.cursor, df: pd.DataFrame) -> None:
        """Insert category hierarchy data."""
        category_data = df[[
            'category_id', 'category_level1', 'category_level2', 'category_level3'
        ]].drop_duplicates().values.tolist()
        
        query = """
            INSERT INTO category_hierarchy 
                (category_id, category_level1, category_level2, category_level3)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (category_id) DO UPDATE
                SET category_level1 = EXCLUDED.category_level1,
                    category_level2 = EXCLUDED.category_level2,
                    category_level3 = EXCLUDED.category_level3
        """
        
        execute_batch(cur, query, category_data, page_size=self.batch_size)

    def generate_product_id_from_name(self, product_name: str) -> int:
        """Generate a product ID from product name by cleaning and hashing."""
        if pd.isna(product_name):
            return None
        # Clean the product name: remove special chars, lowercase, etc.
        cleaned_name = ''.join(e.lower() for e in str(product_name) if e.isalnum())
        # Take first 8 chars of hash and convert to an integer
        hash_value = hashlib.md5(cleaned_name.encode()).hexdigest()[:8]
        # Convert the first 8 characters of the hash to an integer
        return int(hash_value, 16)

    def _insert_products(self, cur: psycopg2.extensions.cursor, df: pd.DataFrame) -> None:
        """Insert product data."""
        product_data = df[[
            'product_id_pk', 'product_id', 'product_name', 'source'
        ]].drop_duplicates().values.tolist()
        
        query = """
            INSERT INTO product 
                (product_id_pk, product_id, product_name, source)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (product_id_pk) DO UPDATE SET
                product_name = EXCLUDED.product_name
        """
        
        execute_batch(cur, query, product_data, page_size=self.batch_size)

    def prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare DataFrame by adding required columns and handling NULL values."""
        df = self.clean_dataframe(df)
        
        # For null product_ids, generate from product_name
        df['product_id'] = df.apply(
            lambda x: x['product_id'] if not pd.isna(x['product_id']) 
            else self.generate_product_id_from_name(x['product_name']),
            axis=1
        )
        
        # Generate IDs
        df['product_id_pk'] = df.apply(
            lambda x: self.generate_product_id_pk(x['product_id'], x['source']),
            axis=1
        )
        
        df['category_id'] = df.apply(
            lambda x: self.generate_category_id(
                x['category_level1'],
                x['category_level2'],
                x['category_level3']
            ),
            axis=1
        )

        # Handle price splitting
        df['price_integer'] = df['product_price'].fillna(0).astype(float).astype(int)
        df['price_decimal'] = ((df['product_price'].fillna(0).astype(float) % 1) * 100).astype(int)
        df['price_currency'] = 'EUR'  # Default currency

        return df

    def insert_data(self, df: pd.DataFrame) -> None:
        """Insert data from DataFrame into PostgreSQL tables."""
        if df.empty:
            self.logger.warning("Empty DataFrame provided, skipping insertion")
            return

        # Prepare the data
        df = self.prepare_data(df)

        with self.connect() as conn:
            with conn.cursor() as cur:
                try:
                    # Filter out rows with NULL primary keys
                    valid_df = df.dropna(subset=['product_id_pk'])
                    
                    if len(valid_df) == 0:
                        self.logger.warning("No valid records to insert after NULL filtering")
                        return
                    
                    # Insert category hierarchy
                    self._insert_category_hierarchy(cur, valid_df)
                    
                    # Insert products
                    self._insert_products(cur, valid_df)
                    
                    # Insert product categories
                    self._insert_product_categories(cur, valid_df)
                    
                    # Insert product pricing
                    self._insert_product_pricing(cur, valid_df)
                    
                    conn.commit()
                    self.logger.info(f"Successfully inserted {len(valid_df)} records")
                    
                except Exception as e:
                    conn.rollback()
                    self.logger.error(f"Error inserting data: {str(e)}")
                    raise

    def _insert_product_categories(self, cur: psycopg2.extensions.cursor, df: pd.DataFrame) -> None:
        """Insert product category relationships."""
        category_data = df[[
            'product_id_pk', 'category_id'
        ]].drop_duplicates().values.tolist()
        
        query = """
            INSERT INTO product_category 
                (product_id_pk, category_id)
            VALUES (%s, %s)
            ON CONFLICT (product_id_pk) DO UPDATE SET
                product_id_pk = EXCLUDED.product_id_pk,
                category_id = EXCLUDED.category_id
        """
        
        execute_batch(cur, query, category_data, page_size=self.batch_size)

    def _insert_product_pricing(self, cur: psycopg2.extensions.cursor, df: pd.DataFrame) -> None:
        """Insert product pricing data."""
        pricing_data = self._convert_to_list(
            df,
            ['product_id_pk', 'price_integer', 'price_decimal', 'price_currency', 'timestamp']
        )
        pricing_data = [row for row in pricing_data if row[0] is not None]
        
        if not pricing_data:
            return
            
        query = """
            INSERT INTO product_pricing 
                (product_id_pk, price_integer, price_decimal, price_currency, timestamp)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (product_id_pk, timestamp) DO UPDATE SET
                product_id_pk = EXCLUDED.product_id_pk,
                timestamp = EXCLUDED.timestamp
        """
        
        execute_batch(cur, query, pricing_data, page_size=self.batch_size)