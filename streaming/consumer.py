import logging
from typing import Optional
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from sqlalchemy.orm import sessionmaker
from .config import settings
from .events import load_avro_schema, avro_msg_to_event
from .load import transform_event_and_persist_to_db

logger = logging.getLogger()


def get_fetch_deserializer():
    registery_client = SchemaRegistryClient(dict(url=str(settings.SCHEMA_REGISTRY_URL)))
    return AvroDeserializer(registery_client, load_avro_schema())  # type: ignore


def get_fetch_consumer():
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
    consumer = get_fetch_consumer()
    deserializer = get_fetch_deserializer()
    processed = 0
    logger.info(
        "Starting fetch consumer on topic=%s group=%s",
        settings.FETCH_TOPIC,
        settings.FETCH_GROUP_ID,
    )

    try:
        while True:
            if max_messages is not None and processed >= max_messages:
                logger.info("Reached max_messages %s, exiting", max_messages)
                break
            msg = consumer.poll(settings.FETCH_CONSUMER_TIMEOUT)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARITIION_EOF:
                    continue
                logger.error("KafkaError: %s", msg.error())

            try:
                logger.info("Received message")
                event = avro_msg_to_event(msg, deserializer)
                transform_event_and_persist_to_db(event, session_factory)
                consumer.commit(msg)
                processed += 1
            except Exception:
                logger.exception(
                    "Error while processing message; leaving offset uncommited"
                )

    except KeyboardInterrupt:
        logger.info("Shutting down consumer (KeyboardInterrupt)")
    finally:
        consumer.close()
        logger.info("Consumer closed, processed a total of %s messages", processed)
