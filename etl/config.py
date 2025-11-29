
"""
etl.config contains environment setup, base types and constants
"""
import os
from dotenv import load_dotenv

load_dotenv()

APP_NAME = os.environ.get("APP_NAME", "Weather API ETL dev")

