"""
etl.load contains logic related to database interations
"""
from datetime import datetime
from typing import Sequence
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import func
from requests.models import Response
from requests.exceptions import Timeout, HTTPError
from .models import WeatherRecord
from .db import FetchMetadata, Observation

def load_observation_rows(data: Sequence[WeatherRecord], session: Session, fetch: FetchMetadata):
    fetch_id = fetch.id
    exclude_columns = set(WeatherRecord.model_fields.keys()) - set(Observation.__table__.columns.keys())
    stmt = insert(Observation).values([
        dict(fetch_id=fetch_id,
             **model.model_dump(exclude=exclude_columns, exclude_unset=True))
        for model in data
    ])
    
    stmt = stmt.on_conflict_do_update(
        constraint="u_loc_time", 
        set_=dict(
            timezone=stmt.excluded.timezone,
            temperature=stmt.excluded.temperature,
            precipitation=stmt.excluded.precipitation,
            wind_speed=stmt.excluded.wind_speed,
            fetch_id=stmt.excluded.fetch_id,
            updated_at=func.now(),
        )   
    )

    session.execute(stmt)

def insert_new_fetch_metadata(url: str, params: dict, session: Session) -> FetchMetadata:
    fetch = FetchMetadata(
        request_timestamp=datetime.utcnow(),
        request_params=params,
        request_url=url
    )

    session.add(fetch)
    session.flush()

    return fetch    

def update_fetch_metadata(metadata: FetchMetadata, session: Session, status_code: int, data: dict) -> FetchMetadata:
    metadata.status = "finished"
    metadata.response_status = status_code
    metadata.response_data = data

    session.add(metadata)
    session.flush()
    return metadata