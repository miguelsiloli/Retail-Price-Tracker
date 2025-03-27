import unittest
import pandas as pd
import numpy as np
from datetime import datetime
from src.db.ingestion.preprocessing import ProductDataStandardizer

class TestProductDataStandardizer(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        self.standardizer = ProductDataStandardizer()
        
        # Sample data for each source
        self.auchan_data = pd.DataFrame({
            'product_id': [1001, 1002, 1003],
            'product_name': ['Apple', 'Banana', 'Orange'],
            'product_price': ['1,99€', '0,89€ / KG', '2.50€'],
            'product_category': ['Fruits', 'Fruits', 'Fruits'],
            'product_category2': ['Fresh', 'Fresh', 'Fresh'],
            'product_category3': ['Organic', np.nan, 'Imported'],
            'product_image': ['img1.jpg', 'img2.jpg', 'img3.jpg'],
            'product_urls': ['url1', 'url2', 'url3'],
            'product_ratings': [4.5, 4.0, 4.8],
            'product_labels': ['Bio', 'Natural', 'Premium'],
            'product_promotions': ['10% off', np.nan, '2 for 1'],
            'source': ['auchan', 'auchan', 'auchan'],
            'timestamp': ['2025-01-06', '2025-01-06', '2025-01-06']
        })
        
        self.continente_data = pd.DataFrame({
            'Product Name': ['Milk', 'Bread', 'Cheese'],
            'Product ID': [2001, 2002, 2003],
            'Price': [0.99, 1.50, 3.25],
            'Price per unit': ['0.99€/L', '0.15€/100g', '32.5€/kg'],
            'Brand': ['Brand1', 'Brand2', 'Brand3'],
            'Category': ['Dairy / Fresh / Milk', 'Bakery / Bread', 'Dairy / Cheese'],
            'Image URL': ['milk.jpg', 'bread.jpg', 'cheese.jpg'],
            'Minimum Quantity': [1, 1, 1],
            'Product Link': ['link1', 'link2', 'link3'],
            'cgid': ['cat1', 'cat2', 'cat3'],
            'tracking_date': ['20250106_013625', '20250106_013625', '20250106_013625'],
            'source': ['continente', 'continente', 'continente']
        })
        
        self.pingo_doce_data = pd.DataFrame({
            'product_name': ['Water', 'Cookies', 'Juice'],
            'product_price': ['0.45€', '1,75€', '1.99€'],
            'product_image': ['water.jpg', 'cookies.jpg', 'juice.jpg'],
            'product_url': ['url1', 'url2', 'url3'],
            'product_rating': [4.2, 4.5, 3.9],
            'source': ['pingo_doce', 'pingo_doce', 'pingo_doce'],
            'timestamp': ['20241113', '20241113', '20241113']
        })

    def test_extract_price(self):
        """Test the extract_price method with various price formats."""
        test_cases = [
            ('1,99€', 1.99),
            ('0,89€ / KG', 0.89),
            ('2.50€', 2.50),
            ('€3,45', 3.45),
            ('10€/KG', 10.0),
            (10.5, 10.5),
            (np.nan, None),
            ('Invalid', None)
        ]
        
        for price_str, expected in test_cases:
            with self.subTest(price_str=price_str):
                result = ProductDataStandardizer.extract_price(price_str)
                self.assertAlmostEqual(result, expected) if expected is not None else self.assertIsNone(result)

    def test_standardize_timestamp(self):
        """Test the standardize_timestamp method with various formats."""
        test_cases = [
            ('2025-01-06', datetime(2025, 1, 6)),
            ('20250106_013625', datetime(2025, 1, 6, 1, 36, 25)),
            ('20241113', datetime(2024, 11, 13))
        ]
        
        for timestamp_str, expected in test_cases:
            with self.subTest(timestamp_str=timestamp_str):
                result = ProductDataStandardizer.standardize_timestamp(timestamp_str)
                self.assertEqual(result, expected)
        
        # Test invalid format
        with self.assertRaises(ValueError):
            ProductDataStandardizer.standardize_timestamp('invalid_format')

    def test_text_to_integer_encoding(self):
        """Test the text_to_integer_encoding method."""
        # Test consistency (same input should produce same output)
        test_str = "Test Product"
        result1 = self.standardizer.text_to_integer_encoding(test_str)
        result2 = self.standardizer.text_to_integer_encoding(test_str)
        self.assertEqual(result1, result2)
        
        # Test different inputs produce different outputs
        result3 = self.standardizer.text_to_integer_encoding("Another Product")
        self.assertNotEqual(result1, result3)
        
        # Test handling of non-string input
        result4 = self.standardizer.text_to_integer_encoding(12345)
        self.assertIsInstance(result4, int)

    def test_split_categories(self):
        """Test the split_categories method."""
        # Create test dataframe with categories
        test_df = pd.DataFrame({
            'Category': ['Dairy / Fresh / Milk', 'Bakery / Bread', 'Dairy / Cheese / Gouda', np.nan]
        })
        
        result_df = ProductDataStandardizer.split_categories(test_df)
        
        # Check that new columns were created
        self.assertIn('category_level1', result_df.columns)
        self.assertIn('category_level2', result_df.columns)
        self.assertIn('category_level3', result_df.columns)
        
        # Check values
        self.assertEqual(result_df.loc[0, 'category_level1'], 'Dairy')
        self.assertEqual(result_df.loc[0, 'category_level2'], 'Fresh')
        self.assertEqual(result_df.loc[0, 'category_level3'], 'Milk')
        
        self.assertEqual(result_df.loc[1, 'category_level1'], 'Bakery')
        self.assertEqual(result_df.loc[1, 'category_level2'], 'Bread')
        self.assertEqual(result_df.loc[1, 'category_level3'], '')
        
        # Check handling of NaN
        self.assertEqual(result_df.loc[3, 'category_level1'], '')
        self.assertEqual(result_df.loc[3, 'category_level2'], '')
        self.assertEqual(result_df.loc[3, 'category_level3'], '')
        
        # Test with DataFrame without 'Category' column
        test_df2 = pd.DataFrame({'OtherColumn': [1, 2, 3]})
        result_df2 = ProductDataStandardizer.split_categories(test_df2)
        self.assertEqual(result_df2.shape, test_df2.shape)

    def test_standardize_auchan_data(self):
        """Test standardization of Auchan data."""
        result_df = self.standardizer.standardize_data(self.auchan_data, 'auchan')
        
        # Check columns
        expected_columns = ['product_id', 'product_name', 'product_price', 'category_level1', 
                           'category_level2', 'category_level3', 'timestamp', 'source']
        self.assertListEqual(list(result_df.columns), expected_columns)
        
        # Check standardized values
        self.assertEqual(result_df.loc[0, 'product_id'], 1001)
        self.assertEqual(result_df.loc[0, 'product_name'], 'Apple')
        self.assertEqual(result_df.loc[0, 'product_price'], 1.99)
        self.assertEqual(result_df.loc[0, 'category_level1'], 'Fruits')
        self.assertEqual(result_df.loc[0, 'category_level2'], 'Fresh')
        self.assertEqual(result_df.loc[0, 'category_level3'], 'Organic')
        self.assertEqual(result_df.loc[0, 'source'], 'auchan')
        self.assertEqual(result_df.loc[0, 'timestamp'], datetime(2025, 1, 6))

    def test_standardize_continente_data(self):
        """Test standardization of Continente data."""
        result_df = self.standardizer.standardize_data(self.continente_data, 'continente')
        
        # Check category splitting
        self.assertEqual(result_df.loc[0, 'category_level1'], 'Dairy')
        self.assertEqual(result_df.loc[0, 'category_level2'], 'Fresh')
        self.assertEqual(result_df.loc[0, 'category_level3'], 'Milk')
        
        # Check price standardization
        self.assertEqual(result_df.loc[0, 'product_price'], 0.99)
        
        # Check timestamp standardization
        self.assertEqual(result_df.loc[0, 'timestamp'], datetime(2025, 1, 6, 1, 36, 25))

    def test_standardize_pingo_doce_data(self):
        """Test standardization of Pingo Doce data."""
        result_df = self.standardizer.standardize_data(self.pingo_doce_data, 'pingo_doce')
        
        # Check product_id generation
        self.assertIsNotNone(result_df.loc[0, 'product_id'])
        self.assertIsInstance(result_df.loc[0, 'product_id'], np.int64)
        
        # Check price standardization
        self.assertEqual(result_df.loc[0, 'product_price'], 0.45)
        self.assertEqual(result_df.loc[1, 'product_price'], 1.75)
        
        # Check timestamp standardization
        self.assertEqual(result_df.loc[0, 'timestamp'], datetime(2024, 11, 13))
        
        # Check empty categories
        self.assertEqual(result_df.loc[0, 'category_level1'], '')
        self.assertEqual(result_df.loc[0, 'category_level2'], '')
        self.assertEqual(result_df.loc[0, 'category_level3'], '')

    def test_edge_cases(self):
        """Test edge cases and error handling."""
        # Empty DataFrame
        empty_df = pd.DataFrame()
        with self.assertRaises(KeyError):
            self.standardizer.standardize_data(empty_df, 'auchan')
        
        # DataFrame with missing required columns
        incomplete_df = pd.DataFrame({'product_id': [1], 'source': ['auchan']})
        with self.assertRaises(KeyError):
            self.standardizer.standardize_data(incomplete_df, 'auchan')
        
        # Invalid source
        with self.assertRaises(KeyError):
            self.standardizer.standardize_data(self.auchan_data, 'invalid_source')

if __name__ == '__main__':
    unittest.main()