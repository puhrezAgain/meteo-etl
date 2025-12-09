import json, uuid
from datetime import datetime
from pathlib import Path
from etl.sources import SourceName, BaseSource
from .config import settings


def save_to_disk(data, fetch_id: uuid.UUID, source_name: SourceName) -> Path:
    now = datetime.now()
    date_path = Path(str(now.year), f"{now.month:02d}", f"{now.day:02d}")
    file_path = settings.RAW_DATA_DIR / date_path / f"{source_name}_{fetch_id}.json"
    file_path.parent.mkdir(parents=True, exist_ok=True)

    with file_path.open("w", encoding="utf-8") as f:
        json.dump(data, f)

    return file_path


def extract_and_save_to_disk(source: BaseSource, fetch_id: uuid.UUID, *args):
    data = source.run_extractor()
    path = save_to_disk(data, fetch_id, source.NAME)
    return data, dict(payload_path=path)
