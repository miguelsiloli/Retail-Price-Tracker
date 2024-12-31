from fastapi.testclient import TestClient
import pytest
from app.main import app

client = TestClient(app)

def test_get_products():
    response = client.get("/api/v1/products/")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert "pagination" in data
    assert isinstance(data["pagination"]["total"], int)

def test_get_products_with_filters():
    response = client.get("/api/v1/products/?name=azeite&source=auchan&page=1&limit=10")
    assert response.status_code == 200
    data = response.json()
    assert len(data["data"]) <= 10

def test_get_product_by_id():
    response = client.get("/api/v1/products/213755")
    assert response.status_code == 200
    data = response.json()
    assert "product_id" in data
    assert "categories" in data
    assert "price_history" in data

def test_get_nonexistent_product():
    response = client.get("/api/v1/products/999999999")
    assert response.status_code == 404