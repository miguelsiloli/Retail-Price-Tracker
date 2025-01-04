from typing import Optional
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, cast, String, and_
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session, joinedload

from core.database import get_db  # pylint: disable=import-error
from models.product import (
    Product,
    ProductPricing,
    CategoryHierarchy,
)  # pylint: disable=import-error
from schemas.product import Product as ProductSchema  # pylint: disable=import-error
from schemas.product import ProductWithPriceHistory  # pylint: disable=import-error
from core.security import oauth2_scheme  # pylint: disable=import-error

router = APIRouter()


@router.get("/", response_model=dict)
async def get_products(
    category_id: Optional[int] = None,
    name: Optional[str] = None,
    source: Optional[str] = None,
    page: int = Query(1, gt=0),
    limit: int = Query(20, gt=0),
    db: Session = Depends(get_db),
):
    """
    Retrieves a paginated list of products, including their latest price,
    best price in the last 30 days, and category information.

    Parameters:
    - category_id (Optional[int]): Filters the products by category ID.
    - name (Optional[str]): Filters the products by their name.
    - source (Optional[str]): Filters the products by their source.
    - page (int): The page number for pagination (default is 1).
    - limit (int): The number of items per page for pagination (default is 20).
    - db (Session): The database session for querying the database.

    Returns:
    - dict: A dictionary containing a list of products with price and category information, and pagination details.

    Pagination Format:
    - data: List of products with their respective details.
    - pagination: Contains the total number of products, current page, and items per page.

    Example Response:
    {
        "data": [
            {
                "product_id_pk": 12345,
                "source": "Amazon",
                "product_name": "Example Product",
                "product_id": "EX123",
                "price": {
                    "last_update": {
                        "current_price": "199.99",
                        "last_updated_on": "2025-01-02T10:00:00"
                    },
                    "best_30_days": {
                        "current_price": "179.99",
                        "last_updated_on": "2024-12-05T14:00:00"
                    },
                    "currency": "€"
                },
                "category": {
                    "category_id": 101,
                    "category_level1": "Electronics",
                    "category_level2": "Laptops",
                    "category_level3": "Gaming"
                }
            },
            ...
        ],
        "pagination": {
            "total": 1000,
            "page": 1,
            "limit": 20
        }
    }

    Raises:
    - HTTPException: If no products match the given filters or if an unexpected error occurs.
    """

    skip = (page - 1) * limit

    # Get latest and best prices in last 30 days
    latest_prices = (
        db.query(
            ProductPricing.product_id_pk,
            ProductPricing.price_integer,
            ProductPricing.price_decimal,
            ProductPricing.timestamp,
        )
        .distinct(ProductPricing.product_id_pk)
        .order_by(ProductPricing.product_id_pk, ProductPricing.timestamp.desc())
        .subquery()
    )

    # Best price in last 30 days
    thirty_days_ago = datetime.now() - timedelta(days=30)
    thirty_days_ago = thirty_days_ago.timestamp()

    best_30_days_prices = (
        db.query(
            ProductPricing.product_id_pk,
            func.min(
                func.concat(
                    cast(ProductPricing.price_integer, String),
                    func.coalesce(
                        func.concat(".", cast(ProductPricing.price_decimal, String)), ""
                    ),
                )
            ).label("best_price"),
            func.min(ProductPricing.timestamp).label("best_price_date"),
        )
        .filter(ProductPricing.timestamp >= thirty_days_ago)
        .group_by(ProductPricing.product_id_pk)
        .subquery()
    )

    query = (
        db.query(Product, latest_prices, best_30_days_prices, CategoryHierarchy)
        .outerjoin(
            latest_prices, Product.product_id_pk == latest_prices.c.product_id_pk
        )
        .outerjoin(
            best_30_days_prices,
            Product.product_id_pk == best_30_days_prices.c.product_id_pk,
        )
        .outerjoin(Product.categories)
    )

    if category_id:
        query = query.filter(CategoryHierarchy.category_id == category_id)
    if name:
        query = query.filter(Product.product_name.ilike(f"%{name}%"))
    if source:
        query = query.filter(Product.source == source)

    total = query.count()

    query = query.add_columns(
        func.concat(
            cast(latest_prices.c.price_integer, String),
            func.coalesce(
                func.concat(".", cast(latest_prices.c.price_decimal, String)), ""
            ),
        ).label("current_price"),
        latest_prices.c.timestamp.label("last_updated_on"),
        best_30_days_prices.c.best_price,
        best_30_days_prices.c.best_price_date,
    )

    products = query.offset(skip).limit(limit).all()

    # Preparing the response data
    product_list = []
    for (
        product,
        _,
        _,
        _,
        _,
        category,
        current_price,
        last_updated_on,
        prod_category,
        _,
        _,
        best_price,
        best_price_date,
    ) in products:
        product_dict = {
            "product_id_pk": product.product_id_pk,
            "source": product.source,
            "product_name": product.product_name,
            "product_id": product.product_id,
            "price": {
                "last_update": {
                    "current_price": current_price,
                    "last_updated_on": datetime.fromtimestamp(
                        last_updated_on
                    ).isoformat()
                    if last_updated_on
                    else None,
                },
                "best_30_days": {
                    "current_price": best_price,
                    "last_updated_on": datetime.fromtimestamp(
                        best_price_date
                    ).isoformat()
                    if best_price_date
                    else None,
                },
                "currency": "€",
            },
            "category": {
                "category_id": prod_category.category_id if prod_category else None,
                "category_level1": prod_category.category_level1 if prod_category else None,
                "category_level2": prod_category.category_level2 if prod_category else None,
                "category_level3": prod_category.category_level3 if prod_category else None,
            },
        }
        product_list.append(product_dict)

    return jsonable_encoder(
        {
            "data": product_list,
            "pagination": {"total": total, "page": page, "limit": limit},
        }
    )


@router.get("/{product_id_pk}", response_model=ProductWithPriceHistory)
async def get_product(product_id_pk: int, db: Session = Depends(get_db)):
    product = (
        db.query(Product)
        .options(joinedload(Product.categories))
        .options(joinedload(Product.prices))
        .filter(Product.product_id_pk == product_id_pk)
        .first()
    )
    """
    Retrieve a product by its primary key (product_id_pk) and return its details along with price history.

    Args:
        product_id_pk (int): The primary key of the product to retrieve.
        db (Session): The database session dependency.

    Raises:
        HTTPException: If the product with the given product_id_pk is not found.

    Returns:
        ProductWithPriceHistory: The detailed information about the product, including its price history.

    Output Schema:
    - `product_id` (int): The unique identifier for the product.
    - `product_id_pk` (int): The primary key for the product.
    - `product_name` (str): The name of the product.
    - `source` (str): The source of the product.
    - `categories` (List[dict]): A list of categories the product belongs to, where each category is represented as:
        - `category_id` (int): The identifier of the category.
        - `level1` (str): The first level category.
        - `level2` (str): The second level category.
        - `level3` (str): The third level category.
    - `price_history` (List[dict]): A list of the product's price history, where each price record contains:
        - `amount` (str): The price of the product as a string in the format "price_integer.price_decimal".
        - `currency` (str): The currency for the price (currently empty).
        - `timestamp` (datetime): The timestamp when the price was recorded.
    """
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")

    return ProductWithPriceHistory(
        product_id=product.product_id,
        product_id_pk=product.product_id_pk,
        product_name=product.product_name,
        source=product.source,
        categories=[
            {
                "category_id": cat.category_id,
                "level1": cat.category_level1,
                "level2": cat.category_level2,
                "level3": cat.category_level3,
            }
            for cat in product.categories
        ],
        price_history=[
            {
                "amount": f"{price.price_integer}.{price.price_decimal:02d}",
                "currency": "",  # currency does not exist yet
                "timestamp": price.timestamp,
            }
            for price in product.prices
        ],
    )
