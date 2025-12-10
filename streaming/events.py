import uuid, json
from typing import cast
from datetime import datetime, timezone
from pathlib import Path
from pydantic import BaseModel, field_serializer, AnyUrl, field_validator
from confluent_kafka import Message
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from etl.db import FetchStatus, FetchMetadata
from .config import settings

SCHEMA_PATH = settings.SCHEMA_PATHS / "fetch_event.avsc"


class FetchEvent(BaseModel):
    fetch_id: uuid.UUID
    source: AnyUrl
    params: dict
    status: FetchStatus
    path: Path
    finished_at: datetime

    @field_serializer("path")
    def serialize_path(self, value: Path) -> str:
        return str(value)

    @field_validator("finished_at", mode="before")
    def normalize_finished_at(cls, v):
        if isinstance(v, (int, float)):
            dt = datetime.fromtimestamp(v / 1000, tz=timezone.utc)
        elif isinstance(v, datetime):
            dt = v
        else:
            raise TypeError(f"Unsupported finished_at type: {type(v)}")

        # assume naive = UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)

        # truncate to milliseconds (Avro timestamp-millis precision)
        us = dt.microsecond
        ms_truncated = (us // 1000) * 1000
        return dt.replace(microsecond=ms_truncated)

    def to_avro(self):
        return dict(
            fetch_id=str(self.fetch_id),
            params=self.params,
            source=str(self.source),
            status=self.status.value,
            path=str(self.path),
            finished_at=int(
                self.finished_at.replace(tzinfo=timezone.utc).timestamp() * 1000
            ),
        )

    @classmethod
    def from_db_model(cls, model: FetchMetadata):
        return cls(
            fetch_id=model.id,
            source=cast(AnyUrl, model.request_url),
            params=model.request_params,
            finished_at=model.finished_at,
            status=model.status,
            path=Path(model.payload_path),
        )


def load_avro_schema():
    with open(SCHEMA_PATH) as f:
        return f.read()


def get_fetch_event_serializer():
    client = SchemaRegistryClient(dict(url=str(settings.SCHEMA_REGISTRY_URL)))

    schema_str = load_avro_schema()

    return AvroSerializer(
        schema_registry_client=client,  # type: ignore
        schema_str=schema_str,  # type: ignore
        to_dict=lambda obj, _: obj.to_avro(),  # type: ignore
    )


def avro_msg_to_event(msg: bytes, deserializer: AvroDeserializer) -> FetchEvent:
    return FetchEvent.model_validate(deserializer(msg))


def get_raw_data_from_fetch_event(event: FetchEvent):
    if not event.path.exists():
        raise FileNotFoundError(f"Raw file does not exist: {event.path}")

    with open(event.path) as f:
        return json.load(f)
