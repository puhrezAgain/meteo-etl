"""
etl.config contains environment setup, base types and constants
"""

import os
from dotenv import load_dotenv
from sqlalchemy.engine.url import URL


load_dotenv()

APP_NAME = os.environ.get("APP_NAME", "Weather API ETL dev")

# STANDARD SQLALCHEMY
DB_URL = os.environ.get("DATABASE_URL") or URL.create(
    drivername="postgresql+psycopg2",
    username=os.environ.get("DB_USER", "etl"),
    password=os.environ.get("DB_PASSWORD", "changme"),
    host=os.environ.get("DB_HOST", "localhost"),
    port=int(os.environ.get("DB_PORT", "5432")),
    database=os.environ.get("DB_NAME", "weather"),
).render_as_string(hide_password=False)

DEBUG = bool(os.environ.get("DEBUG"))
