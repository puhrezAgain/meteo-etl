"""
etl.load contains logic related to database interations
"""
import uuid
from datetime import datetime
from typing import Sequence
from sqlalchemy import func, update
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from .logger import get_logger
from .models import WeatherRecord
from .db import FetchMetadata, Observation

logger = get_logger(__name__)
class LoadError(Exception):
    pass

def load_observation_rows(data: Sequence[WeatherRecord], fetch_id: uuid.UUID, session: Session):
    exclude_columns = set(
        WeatherRecord.model_fields.keys()
        ) - set(Observation.__table__.columns.keys())
    
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
    try: 
        session.execute(stmt)
    except SQLAlchemyError as exc:
        logger.exception("Failed loading observations rows (fetch=%s, rows=%d)", fetch_id, len(data))
        raise LoadError(f"DB weather load failed for fetch_id={fetch_id}") from exc

def insert_fetch_metadata(url: str, params: dict, session: Session) -> uuid.UUID:
    stmt = (
        insert(FetchMetadata)
        .values(request_url=url, request_params=params, request_timestamp=datetime.utcnow())
        .returning(FetchMetadata.id)
    )
    try:
        return session.execute(stmt).scalar_one()
    except SQLAlchemyError as exc:
        logger.exception("Failed loading fetch metadata (url=%s)", url)
        raise LoadError(f"DB fetch create failed for url={url}") from exc

   

def update_fetch_metadata(
        fetch_id: uuid.UUID, status_code: int, data: dict, status: str, session: Session) -> uuid.UUID:
    stmt = (
        update(FetchMetadata)
        .where(FetchMetadata.id == fetch_id)
        .values(status=status, response_status=status_code, response_data=data)
        .returning(FetchMetadata.id)
    )
  
    try:
        return session.execute(stmt).scalar_one()
    except SQLAlchemyError as exc:
        logger.exception("Failed updating fetch metadata (id=%s)", fetch_id)
        raise LoadError(f"DB fetch update failed for fetch_id={fetch_id}") from exc