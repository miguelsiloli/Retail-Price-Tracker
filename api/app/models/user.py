from sqlalchemy import Column, Integer, String
from core.database import Base  # pylint: disable=import-error


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True, index=True)
    hashed_password = Column(String)
