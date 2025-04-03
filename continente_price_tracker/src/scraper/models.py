from typing import List, Optional, Dict, Any, Set, ClassVar, Type
from pydantic import BaseModel, Field, HttpUrl, validator, field_validator, root_validator, model_validator
from datetime import datetime
from functools import wraps

class ConfigurableFieldsModel(BaseModel):
    """Base model that allows configuring which fields are required at runtime."""
    
    # Default required fields that all inheriting models must have
    _base_required_fields: ClassVar[Set[str]] = set()
    
    # Fields required for this specific instance (can be set at creation time)
    _instance_required_fields: Set[str] = set()
    
    @model_validator(mode='before')
    @classmethod
    def validate_required_fields(cls, data):
        """Validate that all required fields are present."""
        if not isinstance(data, dict):
            return data
            
        # Combine class and instance required fields
        all_required = cls._base_required_fields.copy()
        
        # Get instance required fields from input if provided
        instance_required = data.pop('required_fields', set())
        if isinstance(instance_required, (list, tuple, set)):
            all_required.update(instance_required)
        
        # Check that all required fields are present and not None
        missing = []
        for field in all_required:
            if field not in data or data[field] is None:
                missing.append(field)
                
        if missing:
            raise ValueError(f"Missing required fields: {', '.join(missing)}")
            
        return data

class StagingProduct(ConfigurableFieldsModel):
    """
    Canonical staging schema for product data extracted by the scraper.
    Uses Pydantic for validation with configurable required fields.
    """
    # Define default required fields for this model
    _base_required_fields: ClassVar[Set[str]] = {'product_id', 'source_product_id', 'source', 'scraped_timestamp'}
    
    # Define all fields with their metadata
    product_id: str = Field(description="Unique identifier for the product within the source context.")
    source_product_id: str = Field(description="The original product ID/SKU from the source.")
    product_name: Optional[str] = Field(None, description="Product name/title.")
    brand: Optional[str] = Field(None, description="Brand name, as extracted.")
    price_current: Optional[float] = Field(None, description="Current selling price.")
    price_regular: Optional[float] = Field(None, description="Regular/original price.")
    currency: Optional[str] = Field(None, description="ISO 4217 currency code.")
    price_unit_str: Optional[str] = Field(None, description="Raw string representing price per unit.")
    unit_quantity_str: Optional[str] = Field(None, description="Raw string representing the quantity/size.")
    category_raw: Optional[str] = Field(None, description="Original, unparsed category representation.")
    category_list: Optional[List[str]] = Field(None, description="List of categories if parsed directly.")
    category_l1: Optional[str] = Field(None, description="Directly parsed L1 category.")
    category_l2: Optional[str] = Field(None, description="Directly parsed L2 category.")
    category_l3: Optional[str] = Field(None, description="Directly parsed L3 category.")
    product_url: Optional[HttpUrl] = Field(None, description="Absolute URL to the main product page.")
    product_urls_raw: Optional[str] = Field(None, description="Raw string of related URLs if found.")
    image_urls: Optional[List[HttpUrl]] = Field(default_factory=list, description="List of absolute image URLs.")
    source: str = Field(description="Unique identifier for the data source.")
    scraped_timestamp: datetime = Field(description="UTC timestamp of scraping.")
    is_available: Optional[bool] = Field(None, description="Availability status, if extractable.")
    attributes_raw: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Catch-all for extra attributes.")

    model_config = {
        'extra': 'forbid',  # Ensure no extra fields are added accidentally
        'populate_by_name': True  # Allow populating by field name
    }
    
    @classmethod
    def create_with_required(cls, required_fields: Set[str], **data):
        """Factory method to create a StagingProduct with custom required fields."""
        return cls(required_fields=required_fields, **data)