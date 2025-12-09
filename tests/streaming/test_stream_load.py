import pytest, json, uuid
from etl.db import Observation, FetchStatus, FetchMetadata
from etl.sources import SourceName
from etl.app import etl
from streaming.load import save_to_disk, extract_and_save_to_disk


@pytest.mark.integration
def test_save_to_disk(temp_lake_dir, db_session, meteo_payload):
    path = save_to_disk(meteo_payload, uuid.uuid4(), SourceName.METEO)
    assert db_session.query(Observation).count() == 0

    files = list(temp_lake_dir.rglob("*.json"))
    assert len(files) == 1
    assert path in files

    with files[0].open() as f:
        assert json.load(f) == meteo_payload


@pytest.mark.integration
def test_et_with_extract_and_save_to_disk(
    temp_lake_dir, db_session_maker, db_session, override_meteo_api, meteo_payload
):
    fetch_id, status, _ = etl(
        3, 5, SourceName.METEO, db_session_maker, fetch_job=extract_and_save_to_disk
    )
    fetch = db_session.get(FetchMetadata, fetch_id)
    assert status == FetchStatus.SUCCESS
    file_names = list(str(p) for p in temp_lake_dir.rglob("*.json"))
    assert len(file_names) == 1
    assert fetch.payload_path in file_names
    with open(fetch.payload_path) as f:
        assert json.load(f) == meteo_payload
