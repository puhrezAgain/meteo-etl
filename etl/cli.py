"""
WEATHER ETL - A small CLI tool to ingest, transform and store weather data for a particular location
"""

import typer
import json
from rich import print as rich_print
from rich.pretty import Pretty
from .app import et, etl
from .logger import get_logger
from .sources import SourceName

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
    data = et(long, lat, source)

    if pretty:
        rich_print(
            Pretty([model.model_dump() for model in data], indent_guides=True)
        )
    else:
        print(json.dumps([model.model_dump_json() for model in data]))



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
    etl(long, lat, source)
    
def main():
    app()