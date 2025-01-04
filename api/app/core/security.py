from datetime import datetime, timedelta
from typing import Optional
from jose import jwt
from passlib.context import CryptContext
from fastapi.security import OAuth2PasswordBearer

SECRET_KEY = "secretkey1234"  # Change this!
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/v1/token")


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Verifies if the provided plain password matches the hashed password.

    Parameters:
    - plain_password (str): The plain text password provided by the user.
    - hashed_password (str): The hashed password stored in the database.

    Returns:
    - bool: Returns `True` if the plain password matches the hashed password, otherwise `False`.

    Example:
    >>> verify_password("password123", "$2b$12$...")
    True
    """
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    """
    Hashes the provided password using a secure hashing algorithm.

    Parameters:
    - password (str): The plain text password to be hashed.

    Returns:
    - str: The hashed password that can be stored in the database.

    Example:
    >>> get_password_hash("password123")
    "$2b$12$..."
    """
    return pwd_context.hash(password)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """
    Generates a JWT access token for the given data with an optional expiration time.

    Parameters:
    - data (dict): The payload (claims) to be included in the token.
    - expires_delta (Optional[timedelta]): The expiration time as a timedelta object (default is 15 minutes).

    Returns:
    - str: The JWT token as a string, which can be sent to the client.

    Example:
    >>> create_access_token({"sub": "user123"})
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
    """
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=15))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
