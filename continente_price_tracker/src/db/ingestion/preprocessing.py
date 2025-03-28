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
        Handles formats:
        - ISO (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
        - YYYYMMDD_HHMMSS
        - YYYYMMDD
        - YYYYMMDD.0 (from potential float conversion)
        """
        # Ensure input is treated as a string and remove leading/trailing whitespace
        # Handle potential pandas NaT or None which become 'NaT' or 'None' as strings
        if pd.isna(timestamp_str):
             raise ValueError("Input timestamp is null or NaT")
        timestamp_str = str(timestamp_str).strip()

        # Handle empty string after stripping
        if not timestamp_str:
            raise ValueError("Input timestamp string is empty")

        # --- Try parsing known formats ---

        # Try ISO format (YYYY-MM-DD or including time YYYY-MM-DD HH:MM:SS etc.)
        # Using dateutil.parser might be more robust if available,
        # but sticking to datetime for now. This handles YYYY-MM-DD directly.
        if '-' in timestamp_str and len(timestamp_str) >= 10:
             try:
                 # Attempt parsing common ISO-like variants
                 # datetime.fromisoformat expects strict ISO 8601
                 # Use strptime for more flexibility if needed, or dateutil.parser
                 if 'T' in timestamp_str or ' ' in timestamp_str and ':' in timestamp_str:
                     # Try parsing with time component
                     try: return datetime.fromisoformat(timestamp_str.replace(' ', 'T'))
                     except ValueError: pass # Fall through if strict ISO fails
                 else:
                     # Try parsing date only
                     try: return datetime.strptime(timestamp_str.split(' ')[0].split('T')[0], "%Y-%m-%d")
                     except ValueError: pass # Fall through
             except ValueError:
                 pass # Failed ISO-like, try other formats

        # Try format YYYYMMDD_HHMMSS
        if '_' in timestamp_str:
            try:
                # Split carefully in case there are multiple underscores (unlikely for this format)
                parts = timestamp_str.split('_', 1)
                if len(parts) == 2 and len(parts[0]) == 8 and parts[0].isdigit() and len(parts[1]) == 6 and parts[1].isdigit():
                     return datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
            except ValueError:
                pass # Failed YYYYMMDD_HHMMSS, try other formats

        # Try format YYYYMMDD.0 (handle potential float conversion)
        # Check specifically for '.0' ending
        if timestamp_str.endswith('.0'):
            try:
                date_part = timestamp_str[:-2] # Remove the '.0'
                if len(date_part) == 8 and date_part.isdigit():
                    return datetime.strptime(date_part, "%Y%m%d")
            except ValueError:
                 pass # Failed YYYYMMDD.0 parsing, try next

        # Try format YYYYMMDD (plain date, must be exactly 8 digits)
        if len(timestamp_str) == 8 and timestamp_str.isdigit():
            try:
                return datetime.strptime(timestamp_str, "%Y%m%d")
            except ValueError:
                pass # Failed plain YYYYMMDD, fall through to error

        # --- If none of the formats matched ---
        raise ValueError(f"Unsupported or invalid timestamp format encountered: '{timestamp_str}'")

    # Dummy method to simulate usage within the class context if needed for testing
    def standardize_data(self, data, folder_name):
        # Example usage if this method were part of the class:
        # data['timestamp'] = data['timestamp'].apply(self.standardize_timestamp)
        pass
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