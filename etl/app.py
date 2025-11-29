"""
etl.app contains our top level service functions
"""

import json
import uuid
import requests
from typing import Sequence
from .load import insert_fetch_metadata, load_observation_rows, update_fetch_finished, LoadError
from .db import SessionLocal
from .models import WeatherRecord
from .sources import create_source, SourceName
from .logger import get_logger

logger = get_logger()


def et(long: float, lat: float, source: SourceName) -> Sequence[WeatherRecord]:
    current_source = create_source(source, dict(longitude=long, latitude=lat))
    logger.info("Starting fetch for params %s, %s using %s", long, lat, current_source.URL)

    data = current_source.extract_and_transform()

    logger.info("Fetch successful")
    return data

def etl(long: float, lat: float, source: SourceName) -> uuid.UUID:
    logger.info("Starting persisted fetch")
    # insert pending fetch row
    current_source = create_source(source, dict(longitude=long, latitude=lat))

    logger.info("Logging fetch for params %s, %s using %s", long, lat, current_source.URL)
    with SessionLocal.begin() as session:
        fetch_id = insert_fetch_metadata(current_source.URL, current_source.params, session)
    
    try:
        data = current_source.extract_and_transform()

        with SessionLocal.begin() as session:
            logger.info("Fetch successful, initiating ingest")
            load_observation_rows(data, fetch_id, session)
            logger.info("Ingest successful, updating metadata")
    except LoadError as load_error:
        logger.exception("Fetch faced load, updating metadata and aborting")
        status_code = 200
        metadata_body = dict(error="Load error")
        error_occurred = load_error
    except requests.exceptions.HTTPError as http_error:
        logger.exception("Fetch faced http error, updating metadata and aborting")
        status_code = getattr(http_error.response, "status_code")
        metadata_body = dict(error=getattr(http_error.response, "text", str(http_error)))
        error_occurred = http_error
    except json.JSONDecodeError as jsonError:
        logger.exception("Fetch faced json error, updating metadata and aborting")
        status_code = 200
        metadata_body = dict(error="Invalid JSON")
        error_occurred = jsonError
    except Exception as exc:
        logger.exception("Unexpected error during fetch extraction, updated metadata and aborting")
        status_code = 500
        metadata_body = dict(error= str(exc), source="internal")
        error_occurred = exc
    else:
        metadata_body = current_source.raw_data
        status_code = 200
        error_occurred = None

    with SessionLocal.begin() as session:
        update_fetch_finished(fetch_id, status_code, metadata_body, session)

    if error_occurred:
        raise error_occurred

    logger.info("Fetch and store complete")
    return fetch_id