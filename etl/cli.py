"""
WEATHER ETL - A small CLI tool to ingest, transform and store weather data for a particular location
"""

import typer
import json
from rich import print as rich_print
from rich.pretty import Pretty
from .app import et, etl
from .db import SessionLocal
from .logger import configure_logger
from .sources import SourceName

app = typer.Typer()


class FetchError(Exception):
    pass


def parse_cli_params(params: list[str]) -> dict:
    extra_params = {}
    for p in params:
        key, s, value = p.partition("=")
        if not key or s == "":
            raise typer.BadParameter(f"Invalid parameter '{p}', expected <key>=<value>")
        extra_params[key] = value
    return extra_params


@app.command()
def fetch(
    long: float = typer.Option(None, help="Longitude of location to fetch"),
    lat: float = typer.Option(None, help="Latitude of location to fetch"),
    pretty: bool = typer.Option(
        True, "--pretty/--no-pretty", help="Pretty-print output"
    ),
    source: SourceName = typer.Option(
        SourceName.METEO, help="API to be used in fetching", case_sensitive=False
    ),
    params: list[str] = typer.Option(
        [],
        "-p",
        "--param",
        help="Repeated able <key>=<value> pairs to pass to the Source API call",
    ),
):
    """
    Fetch using a longitude and latitude to fetch weather information for a particular location from a source API, printing the result
    """
    data = et(long, lat, source, **parse_cli_params(params))

    if pretty:
        rich_print(Pretty([model.model_dump() for model in data], indent_guides=True))
    else:
        print(json.dumps([model.model_dump_json() for model in data]))


@app.command()
def fetch_and_store(
    long: float = typer.Option(None, help="Longitude of location to fetch"),
    lat: float = typer.Option(None, help="Latitude of location to fetch"),
    source: SourceName = typer.Option(
        SourceName.METEO, help="Strategy to be used in fetching", case_sensitive=False
    ),
    params: list[str] = typer.Option(
        [],
        "-p",
        "--param",
        help="Repeated able <key>=<value> pairs to pass to the Source API call",
    ),
):
    """Fetch_and_store extracts and transfrom data from a source api then persists that information in a database

    Args:
        long (float, optional): _description_. Defaults to typer.Option(None, help="Longitude of location to fetch").
        lat (float, optional): _description_. Defaults to typer.Option(None, help="Latitude of location to fetch").
        source (SourceName, optional): _description_. Defaults to typer.Option(SourceName.METEO, help="Source to be used in fetching", case_sensitive=False).
    """
    etl(long, lat, source, SessionLocal, **parse_cli_params(params))


def main():
    configure_logger()
    app()
