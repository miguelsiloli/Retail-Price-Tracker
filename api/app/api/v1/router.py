from fastapi import APIRouter
from api.v1.endpoints import (
    products,
    categories,
    pricing,
)  # pylint: disable=import-error

api_router = APIRouter()

api_router.include_router(products.router, prefix="/products", tags=["products"])
api_router.include_router(categories.router, prefix="/categories", tags=["categories"])
api_router.include_router(
    pricing.router, prefix="/products/{product_id_pk}/prices", tags=["pricing"]
)
