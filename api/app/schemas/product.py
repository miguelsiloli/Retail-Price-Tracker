from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime


class PriceBase(BaseModel):
    amount: str
    currency: str


class PriceCreate(PriceBase):
    pass


class Price(PriceBase):
    timestamp: datetime

    class Config:
        from_attributes = True


class CategoryBase(BaseModel):
    category_id: int
    level1: str
    level2: Optional[str] = None
    level3: Optional[str] = None


class Category(CategoryBase):
    class Config:
        from_attributes = True


class ProductBase(BaseModel):
    product_id: int
    product_name: str
    source: str


class ProductCreate(ProductBase):
    categories: List[int]
    price: PriceCreate


class ProductUpdate(BaseModel):
    product_name: Optional[str] = None
    source: Optional[str] = None
    categories: Optional[List[int]] = None


class Product(ProductBase):
    product_id_pk: int
    categories: List[Category]
    current_price: Optional[Price] = None

    class Config:
        from_attributes = True


class ProductWithPriceHistory(Product):
    price_history: List[Price]
