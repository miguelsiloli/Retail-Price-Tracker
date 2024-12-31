from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from datetime import datetime
from fastapi.encoders import jsonable_encoder

from core.database import get_db
from models.product import Product, ProductPricing
from schemas.product import Price
from core.security import oauth2_scheme

router = APIRouter()

@router.get("/", response_model=dict)
async def get_product_prices(
    product_id: str,
    token: str = Depends(oauth2_scheme),
    from_date: Optional[datetime] = None,
    to_date: Optional[datetime] = None,
    currency: Optional[str] = None,
    db: Session = Depends(get_db)
):
    product = db.query(Product).filter(Product.product_id == product_id).first()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    query = db.query(ProductPricing).filter(ProductPricing.product_id_pk == product.product_id_pk)
    
    if from_date:
        query = query.filter(ProductPricing.timestamp >= from_date)
    if to_date:
        query = query.filter(ProductPricing.timestamp <= to_date)
    if currency:
        query = query.filter(ProductPricing.price_currency == currency)
        
    prices = query.order_by(ProductPricing.timestamp.desc()).all()
    
    # Convert integer and decimal parts to string representation
    price_data = []
    for price in prices:
        amount = f"{price.price_integer}.{price.price_decimal:02d}"
        price_data.append({
            "amount": amount,
            "currency": price.price_currency,
            "timestamp": price.timestamp
        })
    
    return jsonable_encoder({"data": price_data})