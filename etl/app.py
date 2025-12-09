"""
etl.app contains our top level service functions
"""

import json, uuid, requests, logging
from typing import Sequence, NamedTuple, Callable, Any, Concatenate, Tuple
from sqlalchemy.orm import sessionmaker
from .load import (
    insert_fetch_metadata,
    load_observation_rows,
    update_fetch_metadata,
    LoadError,
)
from .models import WeatherRecord, FetchUpdate
from .sources import create_source, SourceName, BaseSource
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


class ETLResult(NamedTuple):
    fetch_id: uuid.UUID
    fetch_status: FetchStatus
    data_loader_result: Any


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


def _extract_and_load(
    source: BaseSource, fetch_id: uuid.UUID, session_factory: sessionmaker
):
    """Helper function for etl to by default load data into a db"""
    data = source.extract_and_transform()
    with session_factory.begin() as session:
        load_observation_rows(data, fetch_id, session)

    return None, dict()


def etl(
    long: float,
    lat: float,
    source: SourceName,
    session_factory: sessionmaker,
    fetch_job: Callable[
        Concatenate[BaseSource, uuid.UUID, ...], Tuple[Any, dict]
    ] = _extract_and_load,
    **extra_params,
) -> ETLResult:
    """
    etl uses a source object to extract and transform data using a long(itude) and lat(itude) along with whatever extraparams
    then passing the instatiated source with fetch_id and session into
    a fetch_job function which returns any data to be returned along with job and metadata for fech

    Returns a tuple of the id of the row describing the job, and return from data loader
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
        logger.info("Fetch successful, initiating ingest")
        data_return, fetch_update_dict = fetch_job(
            current_source, fetch_id, session_factory
        )
        logger.info("Ingest successful, updating metadata")
    except Exception as exc:
        error_occurred = exc
        fetch_status = FetchStatus.ERROR
        status_code, error_msg, error_data = _handle_etl_error(exc)
        data_return = None
        fetch_update_dict = dict()
    else:
        status_code = 200
        fetch_status = FetchStatus.SUCCESS
        error_occurred = None
        error_msg = None
        error_data = None

    fetch_update = FetchUpdate(
        response_status=status_code,
        status=fetch_status,
        error_data=error_data,
        **fetch_update_dict,
    )
    with session_factory.begin() as session:
        update_fetch_metadata(fetch_id, fetch_update, session)

    if error_occurred:
        logger.exception(
            error_msg, extra=dict(long=long, lat=lat, source=source, **extra_params)
        )
        raise ETLError(
            f"Error occurred fetching {fetch_id}", fetch_id=fetch_id
        ) from error_occurred

    logger.info("Fetch and store successful")
    return ETLResult(fetch_id, fetch_status, data_return)


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
