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

class ETLError(Exception):
    pass

def et(long: float, lat: float, source: SourceName, **extra_params) -> Sequence[WeatherRecord]:
    current_source = create_source(source, dict(longitude=long, latitude=lat))
    logger.info("Starting fetch for params %s, %s %s using %s", long, lat, extra_params, current_source.URL)

    try:
        data = current_source.extract_and_transform(**extra_params)
        logger.info("Fetch successful")
        return data
    except requests.exceptions.HTTPError as http_error:
        error_msg = f"Fetch faced http error '{http_error.response.text}', aborting"
        error = http_error
    except json.JSONDecodeError as jsonError:
        error_msg = "Fetch faced json error, aborting"
        error = jsonError
    except Exception as exc:
        error_msg = "Unexpected error during fetch extraction, updated metadata and aborting"
        error = exc

    logger.exception(error_msg, extra=dict(long=long, lat=lat, source=source, **extra_params))
    raise ETLError(f"Error occurred fetching {source}") from error


def etl(long: float, lat: float, source: SourceName, **extra_params) -> uuid.UUID:
    logger.info("Starting persisted fetch")

    current_source = create_source(source, dict(longitude=long, latitude=lat))

    logger.info("Logging fetch for params %s, %s %s using %s", long, lat, extra_params, current_source.URL)
    with SessionLocal.begin() as session:
        fetch_id = insert_fetch_metadata(current_source.URL, current_source.params, session)
    
    try:
        raw_data = current_source.run_extractor(**extra_params)
        status_code = 200
        data = current_source.run_transform()

        with SessionLocal.begin() as session:
            logger.info("Fetch successful, initiating ingest")
            load_observation_rows(data, fetch_id, session)
            logger.info("Ingest successful, updating metadata")
    except LoadError as load_error:
        error_msg = "Fetch faced load error, updating metadata and aborting"
        status_code = 200
        raw_data = dict(error="Load error")
        error_occurred = load_error
    except requests.exceptions.HTTPError as http_error:
        error_msg = "Fetch faced http error, updating metadata and aborting"
        status_code = http_error.response.status_code
        raw_data = dict(error=http_error.response.text)
        error_occurred = http_error
    except json.JSONDecodeError as jsonError:
        error_msg = "Fetch faced json error, updating metadata and aborting"
        status_code = 200
        raw_data = dict(error="Invalid JSON")
        error_occurred = jsonError
    except Exception as exc:
        error_msg = "Unexpected error during fetch extraction, updated metadata and aborting"
        status_code = 500
        raw_data = dict(error= str(exc), source="internal")
        error_occurred = exc
    else:
        error_occurred = None
        error_msg = None

    with SessionLocal.begin() as session:
        update_fetch_finished(fetch_id, status_code, raw_data, session)

    if error_occurred:
        logger.exception(error_msg, extra=dict(long=long, lat=lat, source=source, **extra_params))
        raise ETLError(f"Error occurred fetching {source}") from error_occurred


    logger.info("Fetch and store successful")
    return fetch_id