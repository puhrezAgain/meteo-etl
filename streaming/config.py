"""
streaming.config stores a pydantic settings object to concentrate and validate application configuration
"""

from pathlib import Path
from pydantic import PostgresDsn, AnyUrl
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class Settings(BaseSettings):
    DATABASE_URL: PostgresDsn = PostgresDsn(
        "postgresql+psycopg2://etl:changeme@db:5432/weather"
    )

    # global kafka / schema-registry configuration
    KAFKA_BOOTSTRAP_SERVERS: AnyUrl = AnyUrl("localhost:9092")
    SCHEMA_REGISTRY_URL: AnyUrl = AnyUrl("http://localhost:8081")
    SCHEMA_PATHS: Path = Path(__file__).parent / "schemas"

    # fetch producer / consumer configuration
    FETCH_GROUP_ID: str = "meteo.ingestor.fetch-consumer"
    FETCH_TOPIC: str = "meteo.fetch.v1"
    FETCH_CONSUMER_TIMEOUT: float = 1

    # data lake sink confiuration
    DATA_DIR: Path = PROJECT_ROOT / "data"
    RAW_DATA_DIR: Path = DATA_DIR / "bronze"

    # allow local .env to override example .env
    model_config = SettingsConfigDict(env_file=(".env.example", ".env"), extra="allow")


settings = Settings()
