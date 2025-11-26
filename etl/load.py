"""
etl.load contains logic related to database interations
"""
from datetime import datetime
from typing import Sequence
from .models import WeatherRecord, FetchRecord, PartialFetchRecord
from .db import FetchMetadata, SessionLocal


def load_rows(data: Sequence[WeatherRecord], request_timestamp: datetime):
    pass

def insert_new_fetch_metadata(url: str, params: dict, session):
    fetch = FetchMetadata(
        request_timestamp=datetime.utcnow(),
        request_params=params,
        request_url=url
    )

    session.add(fetch)
    session.flush()

    return fetch    
