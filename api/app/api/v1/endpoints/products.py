from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, cast, String, and_
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session, joinedload
from datetime import datetime

from core.database import get_db
from models.product import Product, ProductPricing, CategoryHierarchy
from schemas.product import Product as ProductSchema
from schemas.product import ProductWithPriceHistory
from core.security import oauth2_scheme

router = APIRouter()

@router.get("/", response_model=dict)
async def get_products(
   category_id: Optional[int] = None,
   token: str = Depends(oauth2_scheme),
   name: Optional[str] = None,
   source: Optional[str] = None,
   page: int = Query(1, gt=0),
   limit: int = Query(20, gt=0),
   db: Session = Depends(get_db)
):
   skip = (page - 1) * limit
   
   # Base query for products with latest prices
   latest_prices = (
       db.query(
           ProductPricing.product_id_pk,
           ProductPricing.price_integer,
           ProductPricing.price_decimal,
           ProductPricing.timestamp
       )
       .distinct(ProductPricing.product_id_pk)
       .order_by(
           ProductPricing.product_id_pk,
           ProductPricing.timestamp.desc()
       )
       .subquery()
   )

   query = (
       db.query(Product, latest_prices)
       .outerjoin(
           latest_prices,
           Product.product_id_pk == latest_prices.c.product_id_pk
       )
   )

   if category_id:
       query = query.join(Product.categories).filter(CategoryHierarchy.category_id == category_id)
   if name:
       query = query.filter(Product.product_name.ilike(f"%{name}%"))
   if source:
       query = query.filter(Product.source == source)

   total = query.count()

   # Add price formatting
   query = query.add_columns(
       func.concat(
           cast(latest_prices.c.price_integer, String),
           func.coalesce(func.concat('.', cast(latest_prices.c.price_decimal, String)), '')
       ).label('current_price'),
       latest_prices.c.timestamp.label('last_updated_on')
   )

   products = query.offset(skip).limit(limit).all()
   print(products)

   product_list = []
   for product, price_data, price_integer, price_decimal, last_updated_on, current_price, _ in products:
       product_dict = product.__dict__
       product_dict['current_price'] = current_price
       product_dict['last_updated_on'] = datetime.fromtimestamp(last_updated_on).isoformat() if last_updated_on else None
       product_list.append(product_dict)

   return jsonable_encoder({
       "data": product_list,
       "pagination": {
           "total": total,
           "page": page,
           "limit": limit
       }
   })

@router.get("/{product_id_pk}", response_model=ProductWithPriceHistory)
async def get_product(
    product_id_pk: int,
    db: Session = Depends(get_db)
):
    product = db.query(Product)\
        .options(joinedload(Product.categories))\
        .options(joinedload(Product.prices))\
        .filter(Product.product_id_pk == product_id_pk)\
        .first()
    
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    return ProductWithPriceHistory(
        product_id=product.product_id,
        product_id_pk=product.product_id_pk,
        product_name=product.product_name,
        source=product.source,
        categories=[{
            "category_id": cat.category_id,
            "level1": cat.category_level1,
            "level2": cat.category_level2,
            "level3": cat.category_level3
        } for cat in product.categories],
        price_history=[{
            "amount": f"{price.price_integer}.{price.price_decimal:02d}",
            "currency": "", # currency does not exist yet
            "timestamp": price.timestamp
        } for price in product.prices]
    )