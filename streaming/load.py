"""streaming.load contains functionality related to loading into a database or saving to disk"""

import json, uuid
from datetime import datetime
from pathlib import Path
from sqlalchemy.orm import sessionmaker
from etl.sources import SourceName, BaseSource, get_source_by_url
from etl.load import load_observation_rows
from .config import settings
from .events import FetchEvent, get_raw_data_from_fetch_event


class StreamLoadError(Exception):
    pass


def save_to_disk(data, fetch_id: uuid.UUID, source_name: SourceName) -> Path:
    """creates a json file in a path determined by configuration, source, fetch_id and date from the data passed in"""
    now = datetime.now()
    date_path = Path(str(now.year), f"{now.month:02d}", f"{now.day:02d}")
    file_path = settings.RAW_DATA_DIR / date_path / f"{source_name}_{fetch_id}.json"
    file_path.parent.mkdir(parents=True, exist_ok=True)

    with file_path.open("w", encoding="utf-8") as f:
        json.dump(data, f)

    return file_path


def extract_and_save_to_disk(source: BaseSource, fetch_id: uuid.UUID, *args):
    """
    runs source's extractor to fetch data from its source and sabes to disk assocaited with fetch_id,
    *args is used to conform to etl's fetch_job parameter structure
    """
    data = source.run_extractor()
    path = save_to_disk(data, fetch_id, source.NAME)
    return data, dict(payload_path=path)


def transform_event_and_persist_to_db(event: FetchEvent, session_factory: sessionmaker):
    """
    fetches the data referenced by event, processes it, and loads them into the database
    """
    # TODO: there's probably a better way to do this? migrate schema to have url (as source of truth)
    # but also source name so that new versions of sources can handle versioned data (perhaps based on url) appropriately
    source_class = get_source_by_url(str(event.source))

    if not source_class:
        raise StreamLoadError(f"Source Class not found for {event.source}")

    raw_data = get_raw_data_from_fetch_event(event)
    data = source_class.transform_raw_to_records(raw_data)

    with session_factory.begin() as session:
        load_observation_rows(data, event.fetch_id, session)
        session.commit()
