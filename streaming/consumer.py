"""
streaming.consumer groups logic around fetch event consumption
"""

import logging
from typing import Optional, Callable, Any
from confluent_kafka import Consumer, KafkaError, Message
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from sqlalchemy.orm import sessionmaker
from .config import settings
from .events import load_fetch_schema, avro_msg_to_event
from .load import transform_event_and_persist_to_db

logger = logging.getLogger()


def get_fetch_deserializer():
    """helper function for instantiating an AvroDeserializer based on our fetch schema"""
    registery_client = SchemaRegistryClient(dict(url=str(settings.SCHEMA_REGISTRY_URL)))
    return AvroDeserializer(registery_client, load_fetch_schema())  # type: ignore


def get_fetch_consumer():
    """helper function for instantiating a kafka consumer with our configuration and topic"""
    consumer = Consumer(
        {
            "bootstrap.servers": str(settings.KAFKA_BOOTSTRAP_SERVERS),
            "group.id": settings.FETCH_GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )

    consumer.subscribe([settings.FETCH_TOPIC])
    return consumer


def poll_and_upsert_msg_to_db(
    max_messages: Optional[int], session_factory: sessionmaker
):
    """
    top-level function to poll kafka and handle each message received,
    max messages is more of a dev helper than logically important

    each avro message handled is deserialized, data is extracted from them
    then transformed noramalized and upserting into a database
    """
    consumer = get_fetch_consumer()
    deserializer = get_fetch_deserializer()
    processed = 0
    logger.info(
        "Starting fetch consumer on topic=%s group=%s",
        settings.FETCH_TOPIC,
        settings.FETCH_GROUP_ID,
    )

    try:
        processed = _loop_poll_messages(
            max_messages,
            lambda: consumer.poll(settings.FETCH_CONSUMER_TIMEOUT),
            lambda msg: avro_msg_to_event(msg.value(), deserializer),
            lambda event: transform_event_and_persist_to_db(event, session_factory),
            lambda msg: consumer.commit(msg),
        )
    except KeyboardInterrupt:
        logger.info("Shutting down consumer (KeyboardInterrupt)")
    finally:
        consumer.close()
        logger.info("Consumer closed, processed a total of %s messages", processed)


def _loop_poll_messages(
    max_messages: Optional[int],
    poll: Callable[[], Message],
    msg_to_event: Callable[[Message], Any],
    job: Callable[[Any], None],
    post_job: Callable[[Message], None],
) -> int:
    """helper function to encapulate looping until limit"""
    processed = 0

    while True:
        if max_messages is not None and processed >= max_messages:
            logger.info("Reached max_messages %s, exiting", max_messages)
            break
        msg = poll()

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARITIION_EOF:
                continue
            logger.error("KafkaError: %s", msg.error())

        try:
            logger.info("Received message")
            event = msg_to_event(msg)
            job(event)
            post_job(msg)
            processed += 1
        except Exception:
            logger.exception(
                "Error while processing message; leaving offset uncommited"
            )
    return processed
