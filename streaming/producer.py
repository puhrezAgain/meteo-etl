"""streaming.producer contains the logic around producing and publishing fetch events"""

import logging, uuid
from confluent_kafka import Producer
from sqlalchemy.orm import Session
from confluent_kafka.serialization import SerializationContext, MessageField
from etl.db import FetchMetadata
from .config import settings
from .events import get_fetch_event_serializer, FetchEvent

logger = logging.getLogger(__name__)


class FetchEventProducer:
    """convinience wrapper for Kafka's producer associating it with our fetch serializer and our fetch topic"""

    def __init__(self):
        """wrapper around instatiating kafka producer with our configuration"""
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
        """wrapper around publishing and flushing event serialized with our topic"""
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
    """top level function for publishing a fetch event from a fetch_id"""
    producer = FetchEventProducer()
    fetch = session.get(FetchMetadata, fetch_id)
    fetch_event = FetchEvent.from_db_model(fetch)
    producer.publish_fetch_event(fetch_event)
