import pytest, uuid, json
from pydantic import AnyUrl
from pathlib import Path
from datetime import datetime
from confluent_kafka.serialization import SerializationContext, MessageField
from etl.sources import SourceName
from etl.db import FetchStatus, Observation
from etl.app import etl
from streaming.config import settings
from streaming.producer import get_fetch_event_serializer, publish_finished_fetch
from streaming.consumer import get_fetch_deserializer, poll_and_upsert_msg_to_db
from streaming.events import avro_msg_to_event, FetchEvent
from streaming.load import extract_and_save_to_disk


@pytest.mark.integration
def test_get_fetch_deserializer_deserializes():
    deserializer = get_fetch_deserializer()

    serializer = get_fetch_event_serializer()

    event = FetchEvent(
        fetch_id=uuid.uuid4(),
        source=AnyUrl("http://test"),
        status=FetchStatus.SUCCESS,
        path=Path(__file__),
        params=dict(longitude=3.0, latitude=5.0),
        finished_at=datetime.utcnow(),
    )
    ctx = SerializationContext(settings.FETCH_TOPIC, MessageField.VALUE)
    encoded = serializer(event, ctx)
    assert encoded

    decoded = avro_msg_to_event(encoded, deserializer)

    assert decoded == event


@pytest.mark.integration
def test_poll_and_upsert_msg_to_db(
    override_meteo_api, db_session, db_session_maker, weather_records
):
    fetch_id, _, _ = etl(
        3.0, 5.0, SourceName.METEO, db_session_maker, fetch_job=extract_and_save_to_disk
    )

    publish_finished_fetch(fetch_id, db_session)

    poll_and_upsert_msg_to_db(1, db_session_maker)
    assert db_session.query(Observation).count() == len(weather_records)
