import logging, uuid
from confluent_kafka import Producer
from sqlalchemy.orm import Session
from confluent_kafka.serialization import SerializationContext, MessageField
from etl.db import FetchMetadata
from .config import settings
from .events import get_fetch_event_serializer, FetchEvent

logger = logging.getLogger(__name__)


class FetchEventProducer:
    def __init__(self):
        self._producer = Producer(
            {"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS}
        )
        self._serializer = get_fetch_event_serializer()

    def _delivery_report(self, err, msg):
        if err is not None:
            logger.exception(f"Delivery failed: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def publish_fetch_event(self, event: FetchEvent):
        value_bytes = self._serializer(
            event, SerializationContext(settings.FETCH_TOPIC, MessageField.VALUE)
        )

        self._producer.produce(
            topic=settings.FETCH_TOPIC,
            value=value_bytes,
            callback=self._delivery_report,
        )

        self._producer.flush()


def publish_finished_fetch(fetch_id: uuid.UUID, session: Session):
    producer = FetchEventProducer()
    fetch = session.get(FetchMetadata, fetch_id)
    fetch_event = FetchEvent.from_db_model(fetch)
    producer.publish_fetch_event(fetch_event)
