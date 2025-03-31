import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock, call
from datetime import datetime
import hashlib

# Import the class to test
from src.db.ingestion.db_connector import PostgresConnector

@pytest.fixture
def connector():
    """Create a PostgresConnector instance with mocked parameters."""
    with patch.dict('os.environ', {
        'HOST': 'test-host',
        'DATABASE_NAME': 'test-db',
        'USER': 'test-user',
        'PASSWORD': 'test-pass'
    }):
        return PostgresConnector(batch_size=100, use_pooler=False)

@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    return pd.DataFrame({
        'product_id': [1001, 1002, None, 1004],
        'product_name': ['Test Product 1', 'Test Product 2', 'Test Product 3', 'Test Product 4'],
        'product_price': [19.99, 5.50, None, 0.99],
        'category_level1': ['Food', 'Electronics', 'Clothing', None],
        'category_level2': ['Fresh', 'Computers', None, ''],
        'category_level3': ['Organic', None, '', 'Sale'],
        'timestamp': [
            datetime(2025, 1, 1),
            datetime(2025, 1, 2), 
            None,
            '2025-01-04'
        ],
        'source': ['auchan', 'continente', 'pingo_doce', 'auchan']
    })

@pytest.fixture
def mock_cursor():
    """Create a mock cursor for database operations."""
    cursor = MagicMock()
    return cursor

@pytest.fixture
def mock_connection():
    """Create a mock connection with cursor."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    return conn

class TestPostgresConnector:
    
    def test_init_configuration(self):
        """Test basic initialization of the connector."""
        with patch.dict('os.environ', {
            'HOST': 'test-host',
            'DATABASE_NAME': 'test-db',
            'USER': 'test-user',
            'PASSWORD': 'test-pass'
        }):
            connector = PostgresConnector(batch_size=200)
            
            # Just verify the batch size was set correctly
            assert connector.batch_size == 200
            # Verify connection_params and connection_string exist
            assert hasattr(connector, 'connection_params')
            assert hasattr(connector, 'connection_string')
    
    def test_connect(self, connector):
        """Test connection to database."""
        mock_conn = MagicMock()
        
        with patch('psycopg2.connect', return_value=mock_conn) as mock_connect:
            conn = connector.connect()
            
            mock_connect.assert_called_once_with(**connector.connection_params)
            assert conn == mock_conn
    
    def test_convert_to_list(self, connector, sample_data):
        """Test conversion of DataFrame to list."""
        columns = ['product_id', 'product_name']
        result = connector._convert_to_list(sample_data, columns)
        
        assert isinstance(result, list)
        assert len(result) == len(sample_data)
        assert result[0] == [1001, 'Test Product 1']
        # Test None handling for NaN values
        assert result[2] == [None, 'Test Product 3']
    
    def test_clean_dataframe(self, connector, sample_data):
        """Test DataFrame cleaning."""
        cleaned_df = connector.clean_dataframe(sample_data)
        
        # Check empty string replacement
        assert pd.isna(cleaned_df.loc[3, 'category_level2'])
        
        # Check price conversion
        assert cleaned_df['product_price'].dtype == float
        
        # Check timestamp conversion to Unix timestamp
        expected_timestamp = int(datetime(2025, 1, 1).timestamp())
        assert cleaned_df.loc[0, 'timestamp'] == expected_timestamp
        
        # Original DataFrame should be unchanged
        assert isinstance(sample_data.loc[0, 'timestamp'], datetime)
    
    def test_generate_product_id_pk(self, connector):
        """Test product ID primary key generation."""
        # Test consistent hashing
        id1 = connector.generate_product_id_pk(1001, 'auchan')
        id2 = connector.generate_product_id_pk(1001, 'auchan')
        assert id1 == id2
        
        # Test different inputs produce different outputs
        id3 = connector.generate_product_id_pk(1002, 'auchan')
        assert id1 != id3
        
        # Test handling of None values
        assert connector.generate_product_id_pk(None, 'auchan') is None
        assert connector.generate_product_id_pk(1001, None) is None
        
        # Test positive integer output
        assert id1 > 0
        # Test 32-bit integer output (less than 2^31)
        assert id1 < 2**31
    
    def test_generate_category_id(self, connector):
        """Test category ID generation."""
        # Test consistent hashing
        id1 = connector.generate_category_id('Food', 'Fresh', 'Organic')
        id2 = connector.generate_category_id('Food', 'Fresh', 'Organic')
        assert id1 == id2
        
        # Test different inputs produce different outputs
        id3 = connector.generate_category_id('Food', 'Frozen', 'Organic')
        assert id1 != id3
        
        # Test handling of None/NaN values
        id4 = connector.generate_category_id('Food', None, 'Organic')
        assert id4 != id1
        
        # Test positive integer output
        assert id1 > 0
        # Test 32-bit integer output (less than 2^31)
        assert id1 < 2**31
    
    def test_generate_product_id_from_name(self, connector):
        """Test product ID generation from name."""
        # Test consistent hashing
        id1 = connector.generate_product_id_from_name('Test Product')
        id2 = connector.generate_product_id_from_name('Test Product')
        assert id1 == id2
        
        # Test case insensitivity and special character handling
        id3 = connector.generate_product_id_from_name('TEST Product!')
        assert id1 == id3
        
        # Test None handling
        assert connector.generate_product_id_from_name(None) is None
    
    def test_prepare_data(self, connector, sample_data):
        """Test data preparation."""
        prepared_df = connector.prepare_data(sample_data)
        
        # Check if product_id was generated for null value
        assert not pd.isna(prepared_df.loc[2, 'product_id'])
        
        # Check if product_id_pk was generated
        assert 'product_id_pk' in prepared_df.columns
        assert not pd.isna(prepared_df.loc[0, 'product_id_pk'])
        
        # Check if category_id was generated
        assert 'category_id' in prepared_df.columns
        assert not pd.isna(prepared_df.loc[0, 'category_id'])
        
        # Check price splitting
        assert 'price_integer' in prepared_df.columns
        assert prepared_df.loc[0, 'price_integer'] == 19
        assert prepared_df.loc[0, 'price_decimal'] == 99
        assert prepared_df.loc[0, 'price_currency'] == 'EUR'
    
    @patch('postgres_connector.PostgresConnector.connect')
    def test_insert_data_empty_df(self, mock_connect, connector):
        """Test insert_data with empty DataFrame."""
        empty_df = pd.DataFrame()
        connector.insert_data(empty_df)
        
        # Should not attempt to connect when DataFrame is empty
        mock_connect.assert_not_called()
    
    @patch('postgres_connector.PostgresConnector.prepare_data')
    @patch('postgres_connector.PostgresConnector.connect')
    def test_insert_data(self, mock_connect, mock_prepare, connector, sample_data, mock_connection):
        """Test insert_data with valid DataFrame."""
        # Setup mocks
        mock_connect.return_value.__enter__.return_value = mock_connection
        mock_prepare.return_value = sample_data.copy()
        
        # Add required columns for insertion
        prepared_data = sample_data.copy()
        prepared_data['product_id_pk'] = [1001, 1002, 1003, 1004]
        prepared_data['category_id'] = [2001, 2002, 2003, 2004]
        mock_prepare.return_value = prepared_data
        
        # Mock the database operation methods
        with patch.multiple(connector,
                           _insert_category_hierarchy=MagicMock(),
                           _insert_products=MagicMock(),
                           _insert_product_categories=MagicMock(),
                           _insert_product_pricing=MagicMock()):
            
            connector.insert_data(sample_data)
            
            # Check if prepare_data was called
            mock_prepare.assert_called_once_with(sample_data)
            
            # Check if connect was called
            mock_connect.assert_called_once()
            
            # Check if all insertion methods were called
            connector._insert_category_hierarchy.assert_called_once()
            connector._insert_products.assert_called_once()
            connector._insert_product_categories.assert_called_once()
            connector._insert_product_pricing.assert_called_once()
            
            # Check if commit was called
            mock_connection.__enter__.return_value.commit.assert_called_once()
    
    @patch('postgres_connector.PostgresConnector.prepare_data')
    @patch('postgres_connector.PostgresConnector.connect')
    def test_insert_data_exception(self, mock_connect, mock_prepare, connector, sample_data, mock_connection):
        """Test insert_data with exception handling."""
        # Setup mocks
        mock_connect.return_value.__enter__.return_value = mock_connection
        mock_prepare.return_value = sample_data.copy()
        
        # Add required columns for insertion
        prepared_data = sample_data.copy()
        prepared_data['product_id_pk'] = [1001, 1002, 1003, 1004]
        prepared_data['category_id'] = [2001, 2002, 2003, 2004]
        mock_prepare.return_value = prepared_data
        
        # Make one of the insertions fail
        with patch.multiple(connector,
                           _insert_category_hierarchy=MagicMock(),
                           _insert_products=MagicMock(side_effect=Exception("Test error")),
                           _insert_product_categories=MagicMock(),
                           _insert_product_pricing=MagicMock()):
            
            with pytest.raises(Exception, match="Test error"):
                connector.insert_data(sample_data)
            
            # Check if rollback was called
            mock_connection.__enter__.return_value.rollback.assert_called_once()
    
    def test_insert_category_hierarchy(self, connector, sample_data, mock_cursor):
        """Test category hierarchy insertion."""
        # Add required columns
        df = sample_data.copy()
        df['category_id'] = [2001, 2002, 2003, 2004]
        
        # Create a side effect to avoid actual execution
        def side_effect(*args, **kwargs):
            # Just verify the SQL string contains what we expect
            sql = args[1]
            assert isinstance(sql, str)
            assert "INSERT INTO category_hierarchy" in sql
            assert "ON CONFLICT (category_id) DO UPDATE" in sql
            return None
            
        with patch('psycopg2.extras.execute_batch', side_effect=side_effect) as mock_execute_batch:
            connector._insert_category_hierarchy(mock_cursor, df)
            
            # Check if execute_batch was called
            mock_execute_batch.assert_called_once()
            
            # Check batch_size parameter
            assert mock_execute_batch.call_args[1]['page_size'] == connector.batch_size
    
    def test_insert_products(self, connector, sample_data, mock_cursor):
        """Test products insertion."""
        # Add required columns
        df = sample_data.copy()
        df['product_id_pk'] = [1001, 1002, 1003, 1004]
        
        # Create a side effect to avoid actual execution
        def side_effect(*args, **kwargs):
            # Just verify the SQL string contains what we expect
            sql = args[1]
            assert isinstance(sql, str)
            assert "INSERT INTO product" in sql
            assert "ON CONFLICT (product_id_pk) DO UPDATE" in sql
            return None
            
        with patch('psycopg2.extras.execute_batch', side_effect=side_effect) as mock_execute_batch:
            connector._insert_products(mock_cursor, df)
            
            # Check if execute_batch was called
            mock_execute_batch.assert_called_once()
            assert mock_execute_batch.call_args[1]['page_size'] == connector.batch_size
    
    def test_insert_product_categories(self, connector, sample_data, mock_cursor):
        """Test product categories insertion."""
        # Add required columns
        df = sample_data.copy()
        df['product_id_pk'] = [1001, 1002, 1003, 1004]
        df['category_id'] = [2001, 2002, 2003, 2004]
        
        # Create a side effect to avoid actual execution
        def side_effect(*args, **kwargs):
            # Just verify the SQL string contains what we expect
            sql = args[1]
            assert isinstance(sql, str)
            assert "INSERT INTO product_category" in sql
            assert "ON CONFLICT (product_id_pk) DO UPDATE" in sql
            return None
            
        with patch('psycopg2.extras.execute_batch', side_effect=side_effect) as mock_execute_batch:
            connector._insert_product_categories(mock_cursor, df)
            
            # Check if execute_batch was called
            mock_execute_batch.assert_called_once()
            assert mock_execute_batch.call_args[1]['page_size'] == connector.batch_size
    
    def test_insert_product_pricing(self, connector, sample_data, mock_cursor):
        """Test product pricing insertion."""
        # Add required columns
        df = sample_data.copy()
        df['product_id_pk'] = [1001, 1002, 1003, 1004]
        df['price_integer'] = [19, 5, None, 0]
        df['price_decimal'] = [99, 50, None, 99]
        df['price_currency'] = ['EUR', 'EUR', 'EUR', 'EUR']
        df['timestamp'] = [1609459200, 1609545600, None, 1609718400]  # Unix timestamps
        
        # Create a side effect to avoid actual execution
        def side_effect(*args, **kwargs):
            # Just verify the SQL string contains what we expect
            sql = args[1]
            assert isinstance(sql, str)
            assert "INSERT INTO product_pricing" in sql
            assert "ON CONFLICT (product_id_pk, timestamp) DO UPDATE" in sql
            return None
            
        with patch('psycopg2.extras.execute_batch', side_effect=side_effect) as mock_execute_batch:
            connector._insert_product_pricing(mock_cursor, df)
            
            # Check if execute_batch was called
            mock_execute_batch.assert_called_once()
            assert mock_execute_batch.call_args[1]['page_size'] == connector.batch_size
    
    def test_insert_product_pricing_no_valid_data(self, connector, mock_cursor):
        """Test product pricing insertion with no valid data."""
        # Create DataFrame with all None product_id_pk
        df = pd.DataFrame({
            'product_id_pk': [None, None],
            'price_integer': [19, 5],
            'price_decimal': [99, 50],
            'price_currency': ['EUR', 'EUR'],
            'timestamp': [1609459200, 1609545600]
        })
        
        # Since we're testing that execute_batch should not be called, 
        # we don't need a side_effect implementation
        with patch('psycopg2.extras.execute_batch') as mock_execute_batch:
            connector._insert_product_pricing(mock_cursor, df)
            
            # Should not execute batch since no valid data
            mock_execute_batch.assert_not_called()