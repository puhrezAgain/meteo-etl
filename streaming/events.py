import uuid
from typing import cast
from datetime import datetime, timezone
from pathlib import Path
from pydantic import BaseModel, field_serializer, AnyUrl
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
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
