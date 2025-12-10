import pytest
from etl.load import insert_fetch_metadata, update_fetch_metadata
from etl.db import FetchStatus
from etl.models import FetchUpdate
from streaming.producer import publish_finished_fetch


@pytest.mark.integration
def test_publish_finished_fetch(
    meteo_topic, avro_consumer, monkeypatch, db_session, avro_deserialize
):
    from streaming.producer import settings

    monkeypatch.setattr(settings, "FETCH_TOPIC", meteo_topic)
    fetch_id = insert_fetch_metadata("http://test", dict(test="True"), db_session)
    db_session.flush()

    update_fetch_metadata(
        fetch_id,
        FetchUpdate(
            response_status=200,
            error_data=dict(test=True),
            status=FetchStatus.SUCCESS,
        ),
        db_session,
    )
    db_session.flush()

    publish_finished_fetch(fetch_id, db_session)

    msg = avro_consumer.poll(10)
    assert msg is not None, "No event received from Kafka"

    decoded = avro_deserialize(msg.value())

    assert decoded["fetch_id"] == str(fetch_id)
