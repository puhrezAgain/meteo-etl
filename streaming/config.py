from typing import cast
from pathlib import Path
from pydantic import PostgresDsn, AnyUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    DATABASE_URL: PostgresDsn = cast(
        PostgresDsn, "postgresql+psycopg2://etl:changeme@db:5432/weather"
    )
    KAFKA_BOOTSTRAP_SERVERS: AnyUrl = cast(AnyUrl, "localhost:29092")
    SCHEMA_REGISTRY_URL: AnyUrl = cast(AnyUrl, "http://localhost:8081")
    FETCH_TOPIC: str = "weather.fetch_raw.v1"

    RAW_DATA_DIR: Path = Path("/data/bronze")

    SCHEMA_PATHS: Path = Path(__file__).parent / "schemas"

    model_config = SettingsConfigDict(env_file=(".env.example", ".env"), extra="allow")


settings = Settings()
