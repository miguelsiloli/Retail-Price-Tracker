from datetime import datetime, timedelta
from fastapi.testclient import TestClient
import pytest
from app.main import app

client = TestClient(app)


def test_get_product_prices():
    response = client.get("/api/v1/products/213755/prices")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data


def test_get_product_prices_with_filters():
    today = datetime.now().date().isoformat()
    week_ago = (datetime.now() - timedelta(days=7)).date().isoformat()

    response = client.get(
        f"/api/v1/products/213755/prices?from_date={week_ago}&to_date={today}"
    )
    assert response.status_code == 200
    data = response.json()
    assert "data" in data


def test_get_prices_nonexistent_product():
    response = client.get("/api/v1/products/999999999/prices")
    assert response.status_code == 404
