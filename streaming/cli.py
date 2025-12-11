"""
streaming.cli contains commands to be users
"""

import typer, logging
from typing import Optional
from etl.cli import parse_cli_params
from etl.logger import configure_logger
from etl.sources import SourceName
from etl.app import etl
from etl.db import SessionLocal
from .config import settings
from .load import extract_and_save_to_disk
from .producer import publish_finished_fetch
from .consumer import poll_and_upsert_msg_to_db

app = typer.Typer()
logger = logging.getLogger(__name__)


@app.callback(invoke_without_command=False)
def root():
    """Only show help when no command is given, otherwise fetch_and_publish is currently run."""
    pass


@app.command()
def fetch_and_publish(
    long: float = typer.Option(..., help="Longitude of location to fetch"),
    lat: float = typer.Option(..., help="Latitude of location to fetch"),
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
    """fetch_and_publish extracts and transfrom data from a source api then persists that information to disk
        saving metadata about the fetch to database and publishing a reference to that event to a queue

    Args:
        long (float, optional): _description_. Defaults to typer.Option(None, help="Longitude of location to fetch").
        lat (float, optional): _description_. Defaults to typer.Option(None, help="Latitude of location to fetch").
        source (SourceName, optional): _description_. Defaults to typer.Option(SourceName.METEO, help="Source to be used in fetching", case_sensitive=False).
    """
    parsed_params = parse_cli_params(params)
    fetch_id, _, _ = etl(
        long,
        lat,
        source,
        SessionLocal,
        fetch_job=extract_and_save_to_disk,
        **parsed_params,
    )

    logging.info(f"Publishing event for fetch {fetch_id}")
    with SessionLocal() as session:
        publish_finished_fetch(fetch_id, session)


@app.command()
def consume_fetch_events(
    max_messages: Optional[int] = typer.Option(
        None,
        help="Optional limit of messages to process before exiting (useful for tests)",
    )
):
    """
    Run a consumer that:
    - Listens for fetch metadata events
    - Reads the raw file for each event
    - Runs transform+upsert on that raw file into the database
    """
    poll_and_upsert_msg_to_db(max_messages, SessionLocal)


def main():
    configure_logger()
    app()
