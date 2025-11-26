
import os
from enum import Enum
from dotenv import load_dotenv

load_dotenv()

APP_NAME = os.environ.get("APP_NAME", "Weather API ETL dev")
class SourceName(str, Enum):
    METEO = "etl.meteo"
