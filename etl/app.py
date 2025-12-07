"""
etl.app contains our top level service functions
"""

import json, uuid, requests, logging
from typing import Sequence, NamedTuple
from sqlalchemy.orm import sessionmaker
from .load import (
    insert_fetch_metadata,
    load_observation_rows,
    update_fetch_metadata,
    LoadError,
)
from .models import WeatherRecord
from .sources import create_source, SourceName
from .db import FetchStatus

logger = logging.getLogger(__name__)


class ETError(Exception):
    pass


class ETLError(ETError):
    def __init__(self, message: str, fetch_id: uuid.UUID, *args) -> None:
        super().__init__(message, *args)
        self.fetch_id = fetch_id


class ErrorMetadata(NamedTuple):
    status_code: int
    error_message: str
    error_data: dict


def et(
    long: float, lat: float, source: SourceName, **extra_params
) -> Sequence[WeatherRecord]:
    """
    et uses a source object to extract and transform data using a long(itude) and lat(itude) along with whatever extraparams
    returns a sequence of pydantic models representing weather records based on the reponse provided by source

    Raises ETLError if there's been a http error, a json decoding error. or anything other error :)
    """
    current_source = create_source(source, dict(longitude=long, latitude=lat))
    logger.info(
        "Starting fetch for params %s, %s %s using %s",
        long,
        lat,
        extra_params,
        current_source.URL,
    )

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
        error_msg = (
            "Unexpected error during fetch extraction, updated metadata and aborting"
        )
        error = exc

    logger.exception(
        error_msg, extra=dict(long=long, lat=lat, source=source, **extra_params)
    )
    raise ETError(f"Error occurred fetching {source}") from error


def etl(
    long: float,
    lat: float,
    source: SourceName,
    session_factory: sessionmaker,
    **extra_params,
) -> uuid.UUID:
    """
    etl uses a source object to extract and transform data using a long(itude) and lat(itude) along with whatever extraparams
    then loads the resulting records into a database

    Returns the id of the row describing the job
    Raises ETLError if any error occurs
    """
    logger.info("Starting persisted fetch")

    current_source = create_source(source, dict(longitude=long, latitude=lat))

    logger.info(
        "Logging fetch for params %s, %s %s using %s",
        long,
        lat,
        extra_params,
        current_source.URL,
    )
    with session_factory.begin() as session:
        fetch_id = insert_fetch_metadata(
            current_source.URL, current_source.params, session
        )

    try:
        raw_data = current_source.run_extractor(**extra_params)
        data = current_source.run_transform()

        with session_factory.begin() as session:
            logger.info("Fetch successful, initiating ingest")
            load_observation_rows(data, fetch_id, session)
            logger.info("Ingest successful, updating metadata")
    except Exception as exc:
        error_occurred = exc
        fetch_status = FetchStatus.ERROR
        status_code, error_msg, raw_data = _handle_etl_error(exc)
    else:
        status_code = 200
        fetch_status = FetchStatus.SUCCESS
        error_occurred = None
        error_msg = None

    with session_factory.begin() as session:
        update_fetch_metadata(fetch_id, status_code, raw_data, fetch_status, session)

    if error_occurred:
        logger.exception(
            error_msg, extra=dict(long=long, lat=lat, source=source, **extra_params)
        )
        raise ETLError(
            f"Error occurred fetching {fetch_id}", fetch_id=fetch_id
        ) from error_occurred

    logger.info("Fetch and store successful")
    return fetch_id


def _handle_etl_error(error: Exception) -> ErrorMetadata:
    """
    _handle_etl_error is a helper function for handling errors

    Returning a tuple(status_code, error message, error metadata)
    """
    match error:
        case LoadError():
            return ErrorMetadata(
                200,
                "Fetch faced load error, updating metadata and aborting",
                dict(error="Load error"),
            )
        case requests.exceptions.HTTPError():
            return ErrorMetadata(
                error.response.status_code,
                "Fetch faced http error, updating metadata and aborting",
                dict(error=error.response.text),
            )
        case json.JSONDecodeError():
            return ErrorMetadata(
                200,
                "Fetch faced json error, updating metadata and aborting",
                dict(error="Invalid JSON"),
            )
        case _:
            return ErrorMetadata(
                500,
                "Unexpected error during fetch extraction, updated metadata and aborting",
                dict(error=str(error), source="internal"),
            )
