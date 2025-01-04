from typing import Optional
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from fastapi.encoders import jsonable_encoder

from core.database import get_db  # pylint: disable=import-error
from models.product import Product, ProductPricing  # pylint: disable=import-error
from schemas.product import Price  # pylint: disable=import-error
from core.security import oauth2_scheme  # pylint: disable=import-error

router = APIRouter()


@router.get("/", response_model=dict)
async def get_product_prices(
    product_id_pk: str,
    from_date: Optional[datetime] = None,
    to_date: Optional[datetime] = None,
    currency: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """
    Retrieves the price history for a specific product, optionally filtered by date range and currency.

    Parameters:
    - product_id_pk (str): The primary key of the product for which to retrieve price history.
    - from_date (Optional[datetime]): The start date for filtering prices (optional).
    - to_date (Optional[datetime]): The end date for filtering prices (optional).
    - currency (Optional[str]): The currency to filter prices by (optional).
    - db (Session): The database session for querying the database.

    Returns:
    - dict: A dictionary containing a list of prices and timestamps for the given product, with optional filters applied.

    Example Response:
    {
        "data": [
            {
                "amount": "199.99",
                "currency": "USD",
                "timestamp": "2025-01-02T10:00:00"
            },
            {
                "amount": "179.99",
                "currency": "USD",
                "timestamp": "2024-12-05T14:00:00"
            },
            ...
        ]
    }

    Raises:
    - HTTPException: If the product with the given product_id_pk is not found.
    """

    # Retrieve the product by its product_id_pk
    product = db.query(Product).filter(Product.product_id_pk == product_id_pk).first()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")

    # Build the query for the price history of the product
    query = db.query(ProductPricing).filter(
        ProductPricing.product_id_pk == product.product_id_pk
    )

    # Apply date range filters if provided
    if from_date:
        query = query.filter(ProductPricing.timestamp >= from_date)
    if to_date:
        query = query.filter(ProductPricing.timestamp <= to_date)

    # Apply currency filter if provided
    if currency:
        query = query.filter(ProductPricing.price_currency == currency)

    # Retrieve the price data ordered by timestamp in descending order
    prices = query.order_by(ProductPricing.timestamp.desc()).all()

    # Convert integer and decimal parts to string representation for display
    price_data = []
    for price in prices:
        amount = f"{price.price_integer}.{price.price_decimal:02d}"
        price_data.append(
            {
                "amount": amount,
                "currency": price.price_currency,
                "timestamp": price.timestamp,
            }
        )

    return jsonable_encoder({"data": price_data})
