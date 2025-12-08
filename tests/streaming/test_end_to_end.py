import pytest
import json
from etl.db import Observation
from etl.sources import SourceName
from etl.load import insert_fetch_metadata
from streaming.load import save_to_disk


@pytest.mark.integration
def test_etl_with_local_disk_writing(monkeypatch, tmp_path, db_session, meteo_payload):
    from streaming.load import settings
    monkeypatch.setattr(settings, "RAW_DATA_DIR", tmp_path)
    fetch_id = insert_fetch_metadata("test", dict(test=True), db_session)
    
    path = save_to_disk(meteo_payload, fetch_id, SourceName.METEO)
    assert db_session.query(Observation).count() == 0

    files = list(tmp_path.rglob("*.json"))
    assert len(files) == 1
    assert path in files

    with files[0].open() as f:
        assert json.load(f) == meteo_payload

