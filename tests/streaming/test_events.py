import pytest, uuid, json
from pydantic import AnyUrl
from pathlib import Path
from datetime import datetime
from fastavro import schema as fa_schema, validation
from confluent_kafka.serialization import SerializationContext, MessageField
from etl.db import FetchStatus
from etl.sources import SourceName
from streaming.events import load_fetch_schema, FetchEvent, get_fetch_event_serializer
from streaming.config import settings


@pytest.mark.unit
def test_fetch_event_matches_schema():
    schema = fa_schema.parse_schema(json.loads(load_fetch_schema()))
    event = FetchEvent(
        fetch_id=uuid.uuid4(),
        source=AnyUrl("http://test"),
        status=FetchStatus.SUCCESS,
        path=Path(__file__),
        params=dict(longitude=3.0, latitude=5.0),
        finished_at=datetime.utcnow(),
    )

    validation.validate(event.to_avro(), schema)


@pytest.mark.integration
def test_get_fetch_event_serializer_serializes():
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
    encodes = serializer(event, ctx)

    assert isinstance(encodes, bytes)
