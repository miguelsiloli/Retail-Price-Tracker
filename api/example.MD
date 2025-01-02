# Product Catalog API Documentation

## Authorization

All endpoints require Bearer token authentication.

```bash
Authorization: Bearer <your_access_token>
```

To get access token:
```bash
POST /api/v1/token
Content-Type: application/x-www-form-urlencoded

username=your_username&password=your_password
```

Response:
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer"
}
```

## Products

### GET /api/v1/products
List all products with pagination and filters.

Query Parameters:
- `category_id` (optional): Filter by category ID
- `name` (optional): Search by product name
- `source` (optional): Filter by source
- `page` (default: 1): Page number
- `limit` (default: 20): Items per page

Example:
```bash
GET /api/v1/products?name=azeite&source=auchan&page=1&limit=10
```

Response:
```json
{
  "data": [
    {
      "product_id": 433841,
      "product_id_pk": 213755,
      "product_name": "azeite moura dop virgem extra auchan amp cob 750 ml",
      "source": "auchan",
      "categories": [
        {
          "category_id": 123,
          "level1": "Food",
          "level2": "Oils",
          "level3": "Olive Oil"
        }
      ],
      "current_price": {
        "amount": "4.99",
        "currency": "EUR",
        "timestamp": "2024-03-20T14:30:00"
      }
    }
  ],
  "pagination": {
    "total": 45,
    "page": 1,
    "limit": 10
  }
}
```

### GET /api/v1/products/{product_id_pk}
Get detailed product information.

Example:
```bash
GET /api/v1/products/213755
```

Response:
```json
{
  "product_id": 433841,
  "product_id_pk": 213755,
  "product_name": "azeite moura dop virgem extra auchan amp cob 750 ml",
  "source": "auchan",
  "categories": [
    {
      "category_id": 123,
      "level1": "Food",
      "level2": "Oils",
      "level3": "Olive Oil"
    }
  ],
  "price_history": [
    {
      "amount": "4.99",
      "currency": "EUR",
      "timestamp": "2024-03-20T14:30:00"
    },
    {
      "amount": "5.49",
      "currency": "EUR",
      "timestamp": "2024-03-19T14:30:00"
    }
  ]
}
```

## Categories

### GET /api/v1/categories
List all categories with optional level filtering.

Query Parameters:
- `level` (optional): Filter by category level (1, 2, or 3)
- `parent_id` (optional): Filter by parent category

Example:
```bash
GET /api/v1/categories?level=2
```

Response:
```json
{
  "data": [
    {
      "category_id": 123,
      "level1": "Food",
      "level2": "Oils",
      "level3": null
    }
  ]
}
```

### GET /api/v1/categories/{category_id}/products
List products in a specific category.

Query Parameters:
- `page` (default: 1): Page number
- `limit` (default: 20): Items per page

Example:
```bash
GET /api/v1/categories/123/products?page=1&limit=10
```

Response:
```json
{
  "data": [
    {
      "product_id": 433841,
      "product_id_pk": 213755,
      "product_name": "azeite moura dop virgem extra auchan amp cob 750 ml",
      "source": "auchan",
      "current_price": {
        "amount": "4.99",
        "currency": "EUR",
        "timestamp": "2024-03-20T14:30:00"
      }
    }
  ],
  "pagination": {
    "total": 15,
    "page": 1,
    "limit": 10
  }
}
```

## Pricing

### GET /api/v1/products/{product_id}/prices
Get price history for a product.

Query Parameters:
- `from_date` (optional): Filter from date (YYYY-MM-DD)
- `to_date` (optional): Filter to date (YYYY-MM-DD)
- `currency` (optional): Filter by currency

Example:
```bash
GET /api/v1/products/213755/prices?from_date=2024-03-01&to_date=2024-03-20&currency=EUR
```

Response:
```json
{
  "data": [
    {
      "amount": "4.99",
      "currency": "EUR",
      "timestamp": "2024-03-20T14:30:00"
    },
    {
      "amount": "5.49",
      "currency": "EUR",
      "timestamp": "2024-03-19T14:30:00"
    }
  ]
}
```

## Error Responses

All endpoints may return these error responses:

401 Unauthorized:
```json
{
  "detail": "Not authenticated"
}
```

404 Not Found:
```json
{
  "detail": "Resource not found"
}
```