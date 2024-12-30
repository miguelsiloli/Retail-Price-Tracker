import unittest
import sys
import os

# Prepend the specified path to sys.path
# sys.path.insert(0, r'C:\Users\Miguel\Desktop\dataengineeringpr\continente_price_tracking\continente_price_tracker')
# sys.path.append('continente_price_tracker')

import pandas as pd
import numpy as np
from pandas.testing import assert_frame_equal

# Import the functions we want to test
from src.sql.auchan_preprocessing import (
    auchan_product_table,
    auchan_category_table,
    product_category_hierarchy_table,
    auchan_metadata_table
)

class TestAuchanFunctions(unittest.TestCase):

    def setUp(self):
        # Create a sample DataFrame for testing
        self.df = pd.DataFrame({
            'product_id': ['001', '002', '003'],
            'product_name': ['Product A', 'Product B', 'Product C'],
            'product_price': [10.99, 15.99, 20.99],
            'product_category': ['Cat1', 'Cat2', 'Cat1'],
            'product_category2': ['SubCat1', 'SubCat2', 'SubCat3'],
            'product_category3': ['SubSubCat1', 'SubSubCat2', 'SubSubCat3'],
            'product_image': ['image1.jpg', 'image2.jpg', 'image3.jpg'],
            'product_urls': ['url1.com', 'url2.com', 'url3.com'],
            'product_ratings': [4.5, 3.8, 4.2],
            'product_labels': ['Label1', 'Label2', 'Label3'],
            'product_promotions': ['Promo1', 'Promo2', 'Promo3'],
            'source': ['Auchan', 'Auchan', 'Auchan'],
            'timestamp': ['2023-01-01', '2023-01-02', '2023-01-03']
        })

    def test_auchan_product_table(self):
        result = auchan_product_table(self.df)
        expected = pd.DataFrame({
            'product_id': ['001', '002', '003'],
            'product_name': ['Product A', 'Product B', 'Product C'],
            'source': ['Auchan', 'Auchan', 'Auchan']
        })
        assert_frame_equal(result, expected)

    def test_auchan_category_table(self):
        result = auchan_category_table(self.df)
        expected = pd.DataFrame({
            'product_category': ['Cat1', 'Cat2', 'Cat1'],
            'product_category2': ['SubCat1', 'SubCat2', 'SubCat3'],
            'product_category3': ['SubSubCat1', 'SubSubCat2', 'SubSubCat3']
        }).drop_duplicates()
        assert_frame_equal(result, expected)

    def test_product_category_hierarchy_table(self):
        result = product_category_hierarchy_table(self.df)
        expected = pd.DataFrame({
            'product_id': ['001', '002', '003'],
            'source': ['Auchan', 'Auchan', 'Auchan'],
            'product_category': ['Cat1', 'Cat2', 'Cat1'],
            'product_category2': ['SubCat1', 'SubCat2', 'SubCat3'],
            'product_category3': ['SubSubCat1', 'SubSubCat2', 'SubSubCat3']
        })
        assert_frame_equal(result, expected)

    def test_auchan_metadata_table(self):
        result = auchan_metadata_table(self.df)
        expected = pd.DataFrame({
            'product_id_pk': ['001', '002', '003'],
            'brand': [None, None, None],
            'price_per_unit': [None, None, None],
            'minimum_quantity': [None, None, None],
            'product_url': ['url1.com', 'url2.com', 'url3.com'],
            'product_image_url': ['image1.jpg', 'image2.jpg', 'image3.jpg']
        })
        assert_frame_equal(result, expected)

if __name__ == '__main__':
    unittest.main()
