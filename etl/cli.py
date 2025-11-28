"""
WEATHER ETL - A small CLI tool to ingest, transform and store weather data for a particular location
"""

import typer
import json
import requests
from datetime import datetime
from rich import print as rich_print
from rich.pretty import Pretty
from .logger import get_logger
from .constants import SourceName
from .sources import create_source
from .load import insert_fetch_metadata, load_observation_rows, update_fetch_finished, LoadError
from .db import SessionLocal

logger = get_logger()
app = typer.Typer()

class FetchError(Exception):
    pass

@app.command()
def fetch(long: float = typer.Option(None, help="Longitude of location to fetch"),
          lat: float = typer.Option(None, help="Latitude of location to fetch"),
          pretty: bool = typer.Option(True, "--pretty/--no-pretty", help="Pretty-print output"),
          source: SourceName = typer.Option(SourceName.METEO, help="Strategy to be used in fetching", case_sensitive=False)):
    """fetch using a longitude and latitude to fetch weather information for a particular location


    Args:
        long (float, optional): _description_. Defaults to typer.Option(None, help="Longitude of location to fetch").
        lat (float, optional): _description_. Defaults to typer.Option(None, help="Latitude of location to fetch").
        pretty (bool, optional): _description_. Defaults to typer.Option(True, "--pretty/--no-pretty", help="Pretty-print output").
        source (SourceName, optional): _description_. Defaults to typer.Option(SourceName.METEO, help="Source to be used in fetching", case_sensitive=False).
    """
    current_source = create_source(source, dict(longitude=long, latitude=lat))
    logger.info("Starting fetch for params %s, %s using %s", long, lat, current_source.URL)

    data = current_source.extract_and_transform()

    logger.info("Fetch successful")

    if pretty:
        rich_print(
            Pretty([model.model_dump() for model in data], indent_guides=True)
        )
    else:
        print(json.dumps([model.model_dump_json() for model in data]))

    return current_source


@app.command()
def fetch_and_store(
    long: float = typer.Option(None, help="Longitude of location to fetch"),
    lat: float = typer.Option(None, help="Latitude of location to fetch"),
    source: SourceName = typer.Option(SourceName.METEO, help="Strategy to be used in fetching", case_sensitive=False)):
    """fetch_and_store uses `fetch` to extract and transfrom data to then persist that information in a database

    Args:
        long (float, optional): _description_. Defaults to typer.Option(None, help="Longitude of location to fetch").
        lat (float, optional): _description_. Defaults to typer.Option(None, help="Latitude of location to fetch").
        source (SourceName, optional): _description_. Defaults to typer.Option(SourceName.METEO, help="Source to be used in fetching", case_sensitive=False).
    """
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
        finished_fetch = update_fetch_finished(fetch_id, status_code, metadata_body, session)

    if error_occurred:
        raise error_occurred

    logger.info("Fetch and store complete")

def main():
    app()