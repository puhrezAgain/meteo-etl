"""
etl.load contains logic related to database interations
"""
from datetime import datetime
from typing import Sequence
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession
from .models import WeatherRecord, FetchRecord
from .db import FetchMetadata

def load_rows(data: Sequence[WeatherRecord], request_timestamp: datetime):
    pass

async def insert_new_fetch_metadata(session: AsyncSession, fetch_record: FetchRecord):
    stmt = insert(FetchMetadata).values(
        request_timestamp=fetch_record.request_timestamp,
        request_params=fetch_record.request_params,
        request_url=fetch_record.request_url,
    ).returning(FetchMetadata.id)
    res = await session.execute(stmt)
    fetch_id = res.scalar_one()
    await session.commit()
    return fetch_id