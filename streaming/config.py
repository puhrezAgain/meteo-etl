from typing import cast
from pathlib import Path
from pydantic import PostgresDsn, AnyUrl
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class Settings(BaseSettings):
    DATABASE_URL: PostgresDsn = PostgresDsn(
        "postgresql+psycopg2://etl:changeme@db:5432/weather"
    )

    KAFKA_BOOTSTRAP_SERVERS: AnyUrl = AnyUrl("localhost:9092")
    SCHEMA_REGISTRY_URL: AnyUrl = AnyUrl("http://localhost:8081")
    FETCH_TOPIC: str = "meteo.fetch.v1"

    DATA_DIR: Path = PROJECT_ROOT / "data"
    RAW_DATA_DIR: Path = DATA_DIR / "bronze"
    SCHEMA_PATHS: Path = Path(__file__).parent / "schemas"

    model_config = SettingsConfigDict(env_file=(".env.example", ".env"), extra="allow")


settings = Settings()
