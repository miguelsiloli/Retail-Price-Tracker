# Product Catalog API Specification

```
product_catalog_api/
├── alembic/                    # Database migrations
│   └── versions/
├── app/
│   ├── __init__.py
│   ├── main.py                # FastAPI application entry point
│   ├── core/
│   │   ├── __init__.py
│   │   ├── config.py          # Configuration settings
│   │   └── database.py        # Database connection
│   ├── api/
│   │   ├── __init__.py
│   │   ├── deps.py            # Dependencies (DB sessions, auth)
│   │   └── v1/
│   │       ├── __init__.py
│   │       ├── endpoints/
│   │       │   ├── __init__.py
│   │       │   ├── products.py
│   │       │   ├── categories.py
│   │       │   └── pricing.py
│   │       └── router.py
│   ├── models/
│   │   ├── __init__.py
│   │   ├── product.py
│   │   ├── category.py
│   │   └── pricing.py
│   ├── schemas/
│   │   ├── __init__.py
│   │   ├── product.py
│   │   ├── category.py
│   │   └── pricing.py
│   ├── crud/
│   │   ├── __init__.py
│   │   ├── base.py
│   │   ├── product.py
│   │   ├── category.py
│   │   └── pricing.py
│   └── utils/
│       ├── __init__.py
│       └── price_utils.py
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   └── api/
│       └── v1/
│           ├── test_products.py
│           ├── test_categories.py
│           └── test_pricing.py
├── .env
├── .gitignore
├── requirements.txt
├── alembic.ini
└── README.md
```

## Base URL
`/api/v1`

## Endpoints

### Products

#### GET /products
Retrieve a list of products with optional filtering.

Query Parameters:
- `category_id`: Filter by category
- `name`: Search by product name
- `source`: Filter by source
- `page`: Page number (default: 1)
- `limit`: Items per page (default: 20)

Response: 200
```json
{
  "data": [{
    "product_id": "string",
    "product_name": "string",
    "source": "string",
    "categories": [{
      "category_id": "string",
      "level1": "string",
      "level2": "string",
      "level3": "string"
    }],
    "current_price": {
      "amount": "string",
      "currency": "string",
      "timestamp": "string"
    }
  }],
  "pagination": {
    "total": "integer",
    "page": "integer",
    "limit": "integer"
  }
}
```

#### GET /products/{product_id}
Retrieve detailed information about a specific product.

Response: 200
```json
{
  "product_id": "string",
  "product_name": "string",
  "source": "string",
  "categories": [{
    "category_id": "string",
    "level1": "string",
    "level2": "string",
    "level3": "string"
  }],
  "price_history": [{
    "amount": "string",
    "currency": "string",
    "timestamp": "string"
  }]
}
```

#### POST /products
Create a new product.

Request Body:
```json
{
  "product_id": "string",
  "product_name": "string",
  "source": "string",
  "categories": ["string"],
  "price": {
    "amount": "string",
    "currency": "string"
  }
}
```

Response: 201
```json
{
  "product_id": "string",
  "message": "Product created successfully"
}
```

#### PUT /products/{product_id}
Update an existing product.

Request Body:
```json
{
  "product_name": "string",
  "source": "string",
  "categories": ["string"]
}
```

Response: 200
```json
{
  "message": "Product updated successfully"
}
```

### Categories

#### GET /categories
Retrieve the category hierarchy.

Query Parameters:
- `level`: Filter by category level (1, 2, or 3)
- `parent_id`: Filter by parent category

Response: 200
```json
{
  "data": [{
    "category_id": "string",
    "level1": "string",
    "level2": "string",
    "level3": "string"
  }]
}
```

#### GET /categories/{category_id}/products
Retrieve all products in a category.

Query Parameters:
- `page`: Page number (default: 1)
- `limit`: Items per page (default: 20)

Response: 200
```json
{
  "data": [{
    "product_id": "string",
    "product_name": "string",
    "source": "string",
    "current_price": {
      "amount": "string",
      "currency": "string",
      "timestamp": "string"
    }
  }],
  "pagination": {
    "total": "integer",
    "page": "integer",
    "limit": "integer"
  }
}
```

### Pricing

#### POST /products/{product_id}/prices
Add a new price for a product.

Request Body:
```json
{
  "amount": "string",
  "currency": "string"
}
```

Response: 201
```json
{
  "message": "Price added successfully"
}
```

#### GET /products/{product_id}/prices
Get price history for a product.

Query Parameters:
- `from`: Start date
- `to`: End date
- `currency`: Filter by currency

Response: 200
```json
{
  "data": [{
    "amount": "string",
    "currency": "string",
    "timestamp": "string"
  }]
}
```

## Error Responses

### 400 Bad Request
```json
{
  "error": "Bad Request",
  "message": "Invalid input parameters"
}
```

### 404 Not Found
```json
{
  "error": "Not Found",
  "message": "Resource not found"
}
```

### 500 Internal Server Error
```json
{
  "error": "Internal Server Error",
  "message": "An unexpected error occurred"
}
```