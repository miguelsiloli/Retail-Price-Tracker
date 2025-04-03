# continente_price_tracker/tests/test_models.py
import pytest
from pydantic import ValidationError, HttpUrl
from datetime import datetime
import sys
import os

# Add the src directory to the Python path to allow importing the model
# This assumes the tests are run from the 'continente_price_tracker' directory
# or the project root containing it. Adjust if necessary.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

# Try importing the model - handle potential path issues
try:
    from scraper.models import StagingProduct
except ImportError as e:
    pytest.fail(f"Failed to import StagingProduct. Check PYTHONPATH or file location. Error: {e}", pytrace=False)


# --- Test Data ---
VALID_TIMESTAMP = datetime.utcnow() # Use a fixed valid timestamp for tests

VALID_DATA = {
    "product_id": "12345",
    "source_product_id": "SKU-XYZ",
    "product_name": "Test Product",
    "brand": "TestBrand",
    "price_current": 9.99,
    "price_regular": 12.00,
    "currency": "EUR",
    "price_unit_str": "€9.99/kg",
    "unit_quantity_str": "1 kg",
    "category_raw": "Cat1/SubCat2",
    "category_list": ["Cat1", "SubCat2"],
    "category_l1": "Cat1",
    "category_l2": "SubCat2",
    "category_l3": "SubSubCat3",
    "product_url": "https://www.example.com/product/12345",
    "product_urls_raw": '{"api": "/api/p/123"}',
    "image_urls": ["https://www.example.com/image.jpg"],
    "source": "test_source",
    "scraped_timestamp": VALID_TIMESTAMP,
    "is_available": True,
    "attributes_raw": {"color": "red", "size": "L"}
}

# --- Test Cases ---

def test_create_valid_instance():
    """Test successful creation with valid data."""
    try:
        product = StagingProduct(**VALID_DATA)
        # Check a few key fields
        assert product.product_id == "12345"
        assert product.product_name == "Test Product"
        assert product.currency == "EUR"
        assert product.scraped_timestamp == VALID_TIMESTAMP
        assert product.product_url == HttpUrl("https://www.example.com/product/12345")
        assert product.image_urls == [HttpUrl("https://www.example.com/image.jpg")]
    except ValidationError as e:
        pytest.fail(f"Valid data failed validation: {e}")

def test_missing_required_field_error():
    """Test ValidationError when a required field (e.g., product_name) is missing."""
    invalid_data = VALID_DATA.copy()
    del invalid_data["product_name"]
    with pytest.raises(ValidationError) as excinfo:
        StagingProduct(**invalid_data)
    # Check if 'product_name' is mentioned in the error details
    assert "product_name" in str(excinfo.value)
    assert "Field required" in str(excinfo.value) # Pydantic v2 message

def test_invalid_data_type_error():
    """Test ValidationError for incorrect data types."""
    # Test invalid float
    invalid_data_price = VALID_DATA.copy()
    invalid_data_price["price_current"] = "not-a-float"
    with pytest.raises(ValidationError) as excinfo_price:
        StagingProduct(**invalid_data_price)
    assert "price_current" in str(excinfo_price.value)
    assert "Input should be a valid number" in str(excinfo_price.value) # Pydantic v2

    # Test invalid URL
    invalid_data_url = VALID_DATA.copy()
    invalid_data_url["product_url"] = "not-a-valid-url"
    with pytest.raises(ValidationError) as excinfo_url:
        StagingProduct(**invalid_data_url)
    assert "product_url" in str(excinfo_url.value)
    assert "URL scheme not permitted" in str(excinfo_url.value) # Or similar Pydantic v2 message

    # Test invalid datetime
    invalid_data_dt = VALID_DATA.copy()
    invalid_data_dt["scraped_timestamp"] = "yesterday"
    with pytest.raises(ValidationError) as excinfo_dt:
        StagingProduct(**invalid_data_dt)
    assert "scraped_timestamp" in str(excinfo_dt.value)
    assert "Input should be a valid datetime" in str(excinfo_dt.value) # Pydantic v2

def test_invalid_currency_format_error():
    """Test ValidationError for invalid currency format or case."""
    # Test wrong length
    invalid_data_len = VALID_DATA.copy()
    invalid_data_len["currency"] = "EURO"
    with pytest.raises(ValidationError) as excinfo_len:
        StagingProduct(**invalid_data_len)
    assert "currency" in str(excinfo_len.value)
    assert "String should match pattern" in str(excinfo_len.value) # Pydantic v2 pattern error

    # Test wrong characters
    invalid_data_char = VALID_DATA.copy()
    invalid_data_char["currency"] = "E1R"
    with pytest.raises(ValidationError) as excinfo_char:
        StagingProduct(**invalid_data_char)
    assert "currency" in str(excinfo_char.value)
    assert "String should match pattern" in str(excinfo_char.value) # Pydantic v2 pattern error

    # Test wrong case (using custom validator)
    invalid_data_case = VALID_DATA.copy()
    invalid_data_case["currency"] = "eur"
    with pytest.raises(ValidationError) as excinfo_case:
        StagingProduct(**invalid_data_case)
    assert "currency" in str(excinfo_case.value)
    assert "Currency must be uppercase" in str(excinfo_case.value) # Custom validator message

def test_extra_field_error():
    """Test ValidationError when extra fields are provided."""
    invalid_data = VALID_DATA.copy()
    invalid_data["unexpected_field"] = "some value"
    with pytest.raises(ValidationError) as excinfo:
        StagingProduct(**invalid_data)
    assert "unexpected_field" in str(excinfo.value)
    assert "Extra inputs are not permitted" in str(excinfo.value) # Pydantic v2 message