"""
WEATHER ETL - A small CLI tool to ingest, transform and store weather data for a particular location
"""

import typer
import json
import requests
from rich import print as rich_print
from rich.pretty import Pretty
from datetime import datetime
from .logger import get_logger
from .constants import SourceName
from .sources import create_source
from .load import insert_new_fetch_metadata, load_observation_rows, update_fetch_metadata
from .db import SessionLocal

logger = get_logger()
app = typer.Typer()

@app.command()
def fetch(long: float = typer.Option(None, help="Longitude of location to fetch"),
          lat: float = typer.Option(None, help="Latitude of location to fetch"),
          pretty: bool = typer.Option(True, "--pretty/--no-pretty", help="Pretty-print output"),
          should_print: bool = typer.Option(True, "--print/--no-print", help="Print output"),
          source: SourceName = typer.Option(SourceName.METEO, help="Strategy to be used in fetching", case_sensitive=False)):
    """fetch using a longitude and latitude to fetch weather information for a particular location


    Args:
        long (float, optional): _description_. Defaults to typer.Option(None, help="Longitude of location to fetch").
        lat (float, optional): _description_. Defaults to typer.Option(None, help="Latitude of location to fetch").
        pretty (bool, optional): _description_. Defaults to typer.Option(True, "--pretty/--no-pretty", help="Pretty-print output").
        should_print (bool, optional): _description_. Defaults to typer.Option(True, "--print/--no-print", help="Print output").
        source (SourceName, optional): _description_. Defaults to typer.Option(SourceName.METEO, help="Source to be used in fetching", case_sensitive=False).
    """
    current_source = create_source(source, dict(longitude=long, latitude=lat))
    logger.info("Starting fetch for params %s, %s using %s", long, lat, current_source.URL)

    data = current_source.extract_and_transform()

    logger.info("Fetch successful")
    if not should_print:
        return current_source

    if pretty:
        rich_print(
            Pretty(
                [model.model_dump() for model in data], 
                indent_guides=True
                )
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
        pending_fetch_model = insert_new_fetch_metadata(current_source.URL, current_source.params, session)

    data = current_source.extract_and_transform()
    try:
        fetched_source = fetch(long, lat, should_print=False, source=source)
        update_fetch_metadata(pending_fetch_model, session, 200, fetched_source.raw_data)
    except requests.exceptions.HTTPError as httpError:
        logger.warning("Fetch faced http error, updating metadata and aborting")
        update_fetch_metadata(pending_fetch_model, session, 
                              httpError.response.status_code, dict(error=httpError.response.text))
        raise 
    except json.JSONDecodeError as jsonError:
        logger.warning("Fetch faced json error, updating metadata and aborting")
        update_fetch_metadata(pending_fetch_model, session, 200, dict(error="Invalid JSON"))
    
    logger.info("Fetch successful, initiating ingest")

    with SessionLocal.begin() as session:
        load_observation_rows(data, session)

    logger.info("Ingest successful, updating metadata")

    # update fetch with success or failure    
    # load successfully fetched data if fetch success
    logger.info("Fetch and store complete")

def main():
    app()