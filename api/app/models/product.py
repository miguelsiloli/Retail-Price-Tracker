from datetime import datetime
from sqlalchemy import Column, Integer, String, ForeignKey, Table, DateTime
from sqlalchemy.orm import relationship
from core.database import Base  # pylint: disable=import-error

# Association table for product-category relationship
product_category = Table(
    "product_category",
    Base.metadata,
    Column(
        "product_id_pk", Integer, ForeignKey("product.product_id_pk"), primary_key=True
    ),
    Column(
        "category_id",
        Integer,
        ForeignKey("category_hierarchy.category_id"),
        primary_key=True,
    ),
)


class Product(Base):
    __tablename__ = "product"

    product_id_pk = Column(Integer, primary_key=True, index=True)
    product_id = Column(Integer, unique=False, index=False)
    product_name = Column(String)
    source = Column(String)

    categories = relationship(
        "CategoryHierarchy", secondary=product_category, back_populates="products"
    )
    prices = relationship("ProductPricing", back_populates="product")


class CategoryHierarchy(Base):
    __tablename__ = "category_hierarchy"

    category_id = Column(Integer, primary_key=True, index=True)
    category_level1 = Column(String)
    category_level2 = Column(String)
    category_level3 = Column(String)

    products = relationship(
        "Product", secondary=product_category, back_populates="categories"
    )


class ProductPricing(Base):
    __tablename__ = "product_pricing"

    # Remove the 'id' column since it doesn't exist in your database
    product_id_pk = Column(
        Integer, ForeignKey("product.product_id_pk"), primary_key=True
    )
    price_integer = Column(Integer)
    price_decimal = Column(Integer)
    price_currency = Column(String)
    timestamp = Column(DateTime, default=datetime.utcnow, primary_key=True)

    product = relationship("Product", back_populates="prices")
