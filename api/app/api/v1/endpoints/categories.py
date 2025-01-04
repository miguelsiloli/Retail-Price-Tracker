from typing import Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from fastapi.encoders import jsonable_encoder

from core.database import get_db  # pylint: disable=import-error
from models.product import CategoryHierarchy, Product  # pylint: disable=import-error

# from schemas.product import Product as ProductSchema
# from core.security import oauth2_scheme # pylint: disable=import-error

router = APIRouter()


@router.get("/", response_model=dict)
async def get_categories(
    level: Optional[int] = Query(None, ge=1, le=3),
    db: Session = Depends(get_db),
):
    """
    Retrieves categories from the CategoryHierarchy table, optionally filtered by category level (1, 2, or 3).

    Parameters:
    - level (Optional[int]): The category level to filter by. Can be 1, 2, or 3. Defaults to None.
      - Level 1: Top-level categories with no sub-categories (category_level2 is None).
      - Level 2: Categories with sub-categories (category_level2 is not None) but no further sub-categories (category_level3 is None).
      - Level 3: Categories with all levels filled (category_level3 is not None).
    - db (Session): The database session for querying the CategoryHierarchy table.

    Returns:
    - dict: A dictionary containing a list of categories that match the filtering criteria.

    Example Response:
    {
        "data": [
            {
                "category_id": 1,
                "category_level1": "Electronics",
                "category_level2": "Mobile Phones",
                "category_level3": "Smartphones"
            },
            {
                "category_id": 2,
                "category_level1": "Electronics",
                "category_level2": "Laptops",
                "category_level3": "Gaming Laptops"
            },
            ...
        ]
    }

    Raises:
    - HTTPException: If there is any database-related error (e.g., query failure).
    """

    # Start building the query to fetch categories
    query = db.query(CategoryHierarchy)

    # Filter categories based on the provided level
    if level == 1:
        query = query.filter(CategoryHierarchy.category_level2.is_(None))
    elif level == 2:
        query = query.filter(CategoryHierarchy.category_level2.isnot(None))
        query = query.filter(CategoryHierarchy.category_level3.is_(None))
    elif level == 3:
        query = query.filter(CategoryHierarchy.category_level3.isnot(None))

    # Retrieve the matching categories
    categories = query.all()

    # Return the results as a JSON-serializable object
    return jsonable_encoder({"data": categories})


@router.get("/{category_id}/products", response_model=dict)
async def get_category_products(
    category_id: int,
    page: int = Query(1, gt=0),
    limit: int = Query(20, gt=0),
    db: Session = Depends(get_db),
):
    """
    Retrieves a list of products that belong to a specific category, with pagination support.

    Parameters:
    - category_id (int): The ID of the category for which products are being fetched.
    - page (int): The page number for pagination (defaults to 1). The value must be greater than 0.
    - limit (int): The number of products per page (defaults to 20). The value must be greater than 0.
    - db (Session): The database session used for querying the database.

    Returns:
    - dict: A dictionary containing a list of products that belong to the specified category,
            along with pagination information.

    Example Response:
    {
        "data": [
            {
                "product_id_pk": 1,
                "product_name": "Smartphone",
                "source": "Vendor A",
                "category": {
                    "category_id": 1,
                    "category_level1": "Electronics",
                    "category_level2": "Mobile Phones",
                    "category_level3": "Smartphones"
                }
            },
            ...
        ],
        "pagination": {
            "total": 100,
            "page": 1,
            "limit": 20
        }
    }

    Raises:
    - HTTPException: If the category with the given `category_id` does not exist, or if there are any database-related issues.
    """
    skip = (page - 1) * limit

    # Query products for the given category
    query = (
        db.query(Product)
        .join(Product.categories)
        .filter(CategoryHierarchy.category_id == category_id)
    )

    # Calculate the total number of products for the given category
    total = query.count()

    # Fetch the products based on pagination (skip, limit)
    products = query.offset(skip).limit(limit).all()

    # Return the products and pagination data as a JSON-serializable response
    return jsonable_encoder(
        {"data": products, "pagination": {"total": total, "page": page, "limit": limit}}
    )
