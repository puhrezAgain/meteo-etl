import pytest, uuid, json
from pathlib import Path
from datetime import datetime
from etl.db import FetchStatus
from etl.sources import SourceName
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from fastavro import schema as fa_schema, validation
from streaming.events import load_avro_schema, FetchEvent, get_fetch_event_serializer
from streaming.config import settings


@pytest.mark.unit
def test_fetch_event_matches_schema():
    schema = fa_schema.parse_schema(json.loads(load_avro_schema()))
    event = FetchEvent(
        fetch_id=uuid.uuid4(),
        source=SourceName.METEO,
        status=FetchStatus.SUCCESS,
        path=Path(__file__),
        params=dict(longitude=3.0, latitude=5.0),
        finished_at=datetime.utcnow(),
    )

    validation.validate(event.to_avro(), schema)
