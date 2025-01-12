from datetime import datetime
import pandas as pd
from typing import Dict
import hashlib
import re

class ProductDataStandardizer:
    # Field mappings for each source
    FIELD_MAPPINGS = {
        'auchan': {
            'product_id': 'product_id',
            'product_name': 'product_name',
            'product_price': 'product_price',
            'timestamp': 'timestamp',
            'category_level1': 'product_category',
            'category_level2': 'product_category2',
            'category_level3': 'product_category3'
        },
        'pingo_doce': {
            'product_id': 'product_id',
            'product_name': 'product_name',
            'product_price': 'product_price',
            'timestamp': 'timestamp'
        },
        'continente': {
            'product_id': 'Product ID',
            'product_name': 'Product Name',
            'product_price': 'Price',
            'timestamp': 'tracking_date',
            'category': 'Category'
        }
    }

    @staticmethod
    def extract_price(price_str: str) -> float:
        """
        Extract price from string formats like "0,28€ / UN" or "1.99€"
        Handles both comma and dot as decimal separators.
        
        Args:
            price_str: String containing price information
            
        Returns:
            float: Extracted price value
        """
        if pd.isna(price_str):
            return None
            
        price_str = str(price_str).strip()
        
        # If it's already a number, return it
        try:
            return float(price_str)
        except ValueError:
            pass
            
        # Extract price pattern (handles both comma and dot as decimal separator)
        # Pattern explanation:
        # \d+[,.]?\d* - matches numbers with optional decimal part
        # € - matches euro symbol
        # Rest of the string (/ UN, etc.) is ignored
        price_match = re.search(r'(\d+[,.]?\d*)\s*€?', price_str)
        
        if price_match:
            price = price_match.group(1)
            # Replace comma with dot for float conversion
            price = price.replace(',', '.')
            try:
                return float(price)
            except ValueError:
                return None
        
        return None

    @staticmethod
    def standardize_timestamp(timestamp_str: str) -> datetime:
        """
        Convert various timestamp formats to standard datetime.
        """
        timestamp_str = str(timestamp_str).strip()
        
        # Try ISO format (2025-01-06)
        try:
            return datetime.fromisoformat(timestamp_str)
        except ValueError:
            pass
        
        # Try format 20250106_013625
        try:
            if '_' in timestamp_str:
                date_part, time_part = timestamp_str.split('_')
                return datetime.strptime(f"{date_part}_{time_part}", "%Y%m%d_%H%M%S")
        except ValueError:
            pass
        
        # Try format 20241113 (just date)
        try:
            if len(timestamp_str) == 8 and timestamp_str.isdigit():
                return datetime.strptime(timestamp_str, "%Y%m%d")
        except ValueError:
            pass
            
        raise ValueError(f"Unsupported timestamp format: {timestamp_str}")

    def text_to_integer_encoding(self, text: str) -> int:
        text = str(text)
        # Use SHA-256 to ensure deterministic output
        # Take the first 8 bytes of the hash and convert it to an integer
        hash_bytes = hashlib.sha256(text.encode('utf-8')).digest()[:8]  # Get the first 8 bytes
        return int.from_bytes(hash_bytes, byteorder='big', signed=True)

    @staticmethod
    def split_categories(df: pd.DataFrame) -> pd.DataFrame:
        """
        Split category string into three levels for a DataFrame.
        """
        if 'Category' not in df.columns:
            return df
        
        # Split categories and create new columns
        categories = df['Category'].fillna('').str.split('/', expand=True)
        
        # Ensure we have 3 columns
        for i in range(3):
            if i >= categories.shape[1]:
                categories[i] = ''
        
        # Rename columns
        categories = categories[[0, 1, 2]].fillna('')
        categories.columns = ['category_level1', 'category_level2', 'category_level3']
        
        # Strip whitespace
        for col in categories.columns:
            categories[col] = categories[col].str.strip()
        
        return pd.concat([df, categories], axis=1)

    def standardize_data(self, df: pd.DataFrame, source: str) -> pd.DataFrame:
        """
        Standardize DataFrame from any source to common format.
        """

        mapping = self.FIELD_MAPPINGS[source]
        result_df = df.copy()
        
        # Create standard columns
        standard_cols = {
            'product_id': None,
            'product_name': None,
            'product_price': None,
            'category_level1': '',
            'category_level2': '',
            'category_level3': '',
            'timestamp': None,
            'source': None
        }
        
        # Map basic fields
        for standard_field, source_field in mapping.items():
            if source_field in df.columns:
                if standard_field in standard_cols:
                    result_df[standard_field] = df[source_field]
        
        # Handle categories based on source
        if source == 'auchan':
            result_df['category_level1'] = df['product_category'].fillna('')
            result_df['category_level2'] = df['product_category2'].fillna('')
            result_df['category_level3'] = df['product_category3'].fillna('')
        elif source == 'pingo_doce':
            result_df['category_level1'] = ''
            result_df['category_level2'] = ''
            result_df['category_level3'] = ''
            result_df["product_id"] = df["product_name"].apply(self.text_to_integer_encoding)
        elif source == 'continente':
            result_df = self.split_categories(result_df)
        
        # Standardize timestamp
        result_df['timestamp'] = result_df['timestamp'].apply(self.standardize_timestamp)
        
        # Extract and standardize price
        result_df['product_price'] = result_df['product_price'].apply(self.extract_price)
        result_df['product_price'] = pd.to_numeric(result_df['product_price'], errors='coerce').round(2)
        
        # Ensure product_id is integer
        result_df['product_id'] = pd.to_numeric(result_df['product_id'], errors='coerce').astype('Int64')
        result_df['source'] = source
        
        # Select and order final columns
        final_columns = list(standard_cols.keys())
        return result_df[final_columns]