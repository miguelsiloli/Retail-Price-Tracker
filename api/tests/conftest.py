import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fastapi.testclient import TestClient
import sys

sys.path.append(
    "C:\\Users\\Miguel\\Desktop\\dataengineeringpr\\continente_price_tracking\\api"
)

from app.core.database import Base, get_db
from app.main import app

TEST_DATABASE_URL = "postgresql://test_user:test_password@localhost:5432/test_db"


@pytest.fixture(scope="session")
def engine():
    engine = create_engine(TEST_DATABASE_URL)
    return engine


@pytest.fixture(scope="function")
def db_session(engine):
    Base.metadata.create_all(engine)
    TestingSessionLocal = sessionmaker(bind=engine)
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()
        Base.metadata.drop_all(engine)


@pytest.fixture(scope="function")
def client(db_session):
    def override_get_db():
        try:
            yield db_session
        finally:
            db_session.close()

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()
