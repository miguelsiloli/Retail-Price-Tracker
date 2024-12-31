from typing import List, Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from fastapi.encoders import jsonable_encoder

from core.database import get_db
from models.product import CategoryHierarchy, Product
from schemas.product import Category, Product as ProductSchema
from core.security import oauth2_scheme

router = APIRouter()

@router.get("/", response_model=dict)
async def get_categories(
    level: Optional[int] = Query(None, ge=1, le=3),
    token: str = Depends(oauth2_scheme),
    parent_id: Optional[int] = None,
    db: Session = Depends(get_db)
):
    query = db.query(CategoryHierarchy)
    
    if level == 1:
        query = query.filter(CategoryHierarchy.category_level2.is_(None))
    elif level == 2:
        query = query.filter(CategoryHierarchy.category_level2.isnot(None))
        query = query.filter(CategoryHierarchy.category_level3.is_(None))
    elif level == 3:
        query = query.filter(CategoryHierarchy.category_level3.isnot(None))
        
    categories = query.all()
    return jsonable_encoder({"data": categories})

@router.get("/{category_id}/products", response_model=dict)
async def get_category_products(
    category_id: int,
    page: int = Query(1, gt=0),
    limit: int = Query(20, gt=0),
    db: Session = Depends(get_db)
):
    skip = (page - 1) * limit
    
    query = db.query(Product)\
        .join(Product.categories)\
        .filter(CategoryHierarchy.category_id == category_id)
    
    total = query.count()
    products = query.offset(skip).limit(limit).all()
    
    return jsonable_encoder({
        "data": products,
        "pagination": {
            "total": total,
            "page": page,
            "limit": limit
        }
    })