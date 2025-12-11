"""
streaming.consumer groups logic around fetch event consumption
"""

import logging, threading
from typing import Optional, Callable, Any
from confluent_kafka import Consumer, KafkaError, Message
from confluent_kafka.serialization import SerializationError
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from sqlalchemy.orm import sessionmaker
from .config import settings
from .events import load_fetch_schema, avro_msg_to_event
from .load import transform_event_and_persist_to_db

logger = logging.getLogger()


def get_fetch_deserializer():
    """
    Helper function for instantiating an AvroDeserializer based on our fetch schema
    """
    registery_client = SchemaRegistryClient(dict(url=str(settings.SCHEMA_REGISTRY_URL)))
    return AvroDeserializer(registery_client, load_fetch_schema())  # type: ignore


def get_fetch_consumer():
    """
    Helper function for instantiating a kafka consumer with our configuration and topic
    """
    consumer = Consumer(
        {
            "bootstrap.servers": str(settings.KAFKA_BOOTSTRAP_SERVERS),
            "group.id": settings.FETCH_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    consumer.subscribe([settings.FETCH_TOPIC])
    return consumer


def poll_and_upsert_msg_to_db(
    max_messages: Optional[int],
    session_factory: sessionmaker,
    stop_event: Optional[threading.Event] = None,
):
    """
    top-level function to poll kafka and handle each message received,
    max_messages is more of a dev helper than logically important
    stop_event allows a programmatic way to gracefully exit an otherwise infinite loop

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
            consumer.commit,
            stop_event,
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
    commit: Callable[[Message], None],
    stop_event: Optional[threading.Event] = None,
) -> int:
    """
    helper function to encapulate logic around message polling
    max_messages allows for limited runs

    poll is called to get a message,
    msg_to_event to transform that message into something job operates on
    commit performs post job logic, kafka commiting in this usecase

    stop_event allows for graceful exitting of the loop
    """
    processed = 0

    while True:
        if stop_event is not None and stop_event.is_set():
            logger.info("Stop event set, exiting loop")
            break

        if max_messages is not None and processed >= max_messages:
            logger.info("Reached max_messages %s, exiting", max_messages)
            break

        msg = poll()

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            logger.error("KafkaError: %s", msg.error())
            continue

        try:
            logger.info("Received message")
            event = msg_to_event(msg)
            job(event)
            commit(msg)
            processed += 1
        except SerializationError as se:
            logger.exception("Deserialization error: %s; commiting offset", se)
            commit(msg)
        except Exception as e:
            logger.exception(
                "Error while processing message: %s; leaving offset uncommited", e
            )
    return processed
