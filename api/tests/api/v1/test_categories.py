from fastapi.testclient import TestClient
import pytest
from app.main import app

client = TestClient(app)


def test_get_categories():
    response = client.get("/api/v1/categories/")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data


def test_get_categories_with_level():
    response = client.get("/api/v1/categories/?level=1")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data


def test_get_category_products():
    response = client.get("/api/v1/categories/1/products")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert "pagination" in data


def test_get_category_products_with_pagination():
    response = client.get("/api/v1/categories/1/products?page=1&limit=5")
    assert response.status_code == 200
    data = response.json()
    assert len(data["data"]) <= 5
