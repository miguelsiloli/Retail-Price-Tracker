from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """
    Settings class that manages application configuration using environment variables.

    This class is responsible for loading configuration values for the application,
    including database connection details and API settings.

    Attributes:
    - PROJECT_NAME (str): The name of the project.
    - VERSION (str): The version of the project.
    - API_V1_STR (str): The versioned API endpoint path.
    - POSTGRES_SERVER (str): The server address for the PostgreSQL database.
    - POSTGRES_USER (str): The username for the PostgreSQL database.
    - POSTGRES_PASSWORD (str): The password for the PostgreSQL database.
    - POSTGRES_DB (str): The name of the PostgreSQL database.
    - POSTGRES_PORT (str): The port used to connect to the PostgreSQL database.

    Property Methods:
    - SQLALCHEMY_DATABASE_URI (str): Constructs the full database URI for SQLAlchemy.
      This is dynamically generated based on the environment variables for
      the PostgreSQL connection.

    Config:
        - env_file (str): Path to the environment file (".env") that contains sensitive settings.
    """

    PROJECT_NAME: str = "Product Catalog API"
    VERSION: str = "1.0.0"
    API_V1_STR: str = "/api/v1"

    POSTGRES_SERVER: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str
    POSTGRES_PORT: str

    @property
    def SQLALCHEMY_DATABASE_URI(self) -> str:
        """
        Generates the full database URI for SQLAlchemy connection string.

        The connection string follows this pattern:
        postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_SERVER}:{POSTGRES_PORT}/{POSTGRES_DB}

        Returns:
        - str: The fully formatted SQLAlchemy connection URI for PostgreSQL.
        """
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_SERVER}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    class Config:
        """
        Configuration class for loading environment variables.

        Attributes:
        - env_file (str): Specifies the environment file to load configuration from.
        """

        env_file = ".env"


settings = Settings()
