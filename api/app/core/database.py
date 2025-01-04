from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from core.config import settings  # pylint: disable=import-error

engine = create_engine(settings.SQLALCHEMY_DATABASE_URI, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_db():
    """
    Dependency function that provides a database session.

    This function is designed to be used as a dependency in FastAPI endpoints to
    automatically provide a database session (SQLAlchemy Session) and ensure
    it is properly closed after use.

    It is used with FastAPI's dependency injection system to manage database
    sessions and ensure the session is closed even in the event of an exception.

    Yields:
    - db (Session): The SQLAlchemy database session.

    Example:
    - In a FastAPI route:
      ```python
      @router.get("/items")
      async def get_items(db: Session = Depends(get_db)):
          items = db.query(Item).all()
          return items
      ```

    Notes:
    - The `db` session is automatically closed after the route handler has finished executing.
    """
    db = SessionLocal()  # Create a new database session using SessionLocal
    try:
        yield db  # Yield the session to be used in the route handler
    finally:
        db.close()  # Ensure the session is closed after use
