import pytest, uuid, threading, time
from unittest.mock import MagicMock
from pydantic import AnyUrl
from pathlib import Path
from datetime import datetime
from confluent_kafka import KafkaError
from confluent_kafka.serialization import SerializationContext, MessageField
from etl.sources import SourceName
from etl.db import FetchStatus, Observation
from etl.app import etl
from streaming.config import settings
from streaming.producer import get_fetch_event_serializer, publish_finished_fetch
from streaming.consumer import (
    get_fetch_deserializer,
    poll_and_upsert_msg_to_db,
    _loop_poll_messages,
)
from streaming.events import avro_msg_to_event, FetchEvent
from streaming.load import extract_and_save_to_disk


@pytest.mark.integration
def test_get_fetch_deserializer_deserializes():
    deserializer = get_fetch_deserializer()
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
    encoded = serializer(event, ctx)
    assert encoded

    decoded = avro_msg_to_event(encoded, deserializer)

    assert decoded == event


@pytest.mark.integration
def test_poll_and_upsert_msg_to_db(
    override_meteo_api, db_session, db_session_maker, weather_records
):
    fetch_id, _, _ = etl(
        3.0, 5.0, SourceName.METEO, db_session_maker, fetch_job=extract_and_save_to_disk
    )

    publish_finished_fetch(fetch_id, db_session)

    poll_and_upsert_msg_to_db(1, db_session_maker)
    assert db_session.query(Observation).count() == len(weather_records)


def make_mock_msg(value=b"raw", error_obj=None):
    m = MagicMock()
    m.value.return_value = value
    m.error.return_value = error_obj
    return m


@pytest.fixture
def mock_loop_func():
    stop = threading.Event()
    job_func = MagicMock()
    commit_func = MagicMock()
    msg_to_event_func = MagicMock()
    result = {}

    def run_loop(
        msgs,
        result,
        msg_to_event_func=msg_to_event_func,
        job_func=job_func,
        commit_func=commit_func,
    ):
        poll_func = lambda: msgs.pop(0) if msgs else None
        result["processed"] = _loop_poll_messages(
            1, poll_func, msg_to_event_func, job_func, commit_func, stop
        )

    return run_loop, job_func, commit_func, msg_to_event_func, stop, result


@pytest.mark.unit
def test_loop_poll_messages_doesnt_process_error_msg_but_logs(mock_loop_func, caplog):
    msgs = [make_mock_msg(b"test bad avro", error_obj=MagicMock())]
    run_loop, job_func, commit_func, msg_to_event_func, stop, result = mock_loop_func

    with caplog.at_level("ERROR"):
        t = threading.Thread(target=lambda: run_loop(msgs, result), daemon=True)
    t.start()

    time.sleep(0.5)
    stop.set()
    t.join(5)

    assert any("KafkaError" in rec.getMessage() for rec in caplog.records)
    assert not t.is_alive()
    assert result["processed"] == 0
    job_func.assert_not_called()
    commit_func.assert_not_called()
    msg_to_event_func.assert_not_called()


@pytest.mark.unit
def test_loop_poll_messages_doesnt_process_eof_error_msg_and_no_logs(
    mock_loop_func, caplog
):
    error = MagicMock()
    error.code.return_value = KafkaError._PARTITION_EOF
    msgs = [make_mock_msg(b"test bad avro", error_obj=error)]
    run_loop, job_func, commit_func, msg_to_event_func, stop, result = mock_loop_func

    with caplog.at_level("ERROR"):
        t = threading.Thread(target=lambda: run_loop(msgs, result), daemon=True)
    t.start()

    time.sleep(0.5)
    stop.set()
    t.join(5)

    assert not any("KafkaError" in rec.getMessage() for rec in caplog.records)
    assert not t.is_alive()
    assert result["processed"] == 0
    job_func.assert_not_called()
    commit_func.assert_not_called()
    msg_to_event_func.assert_not_called()


@pytest.mark.integration
def test_loop_poll_message_deserialization_error(mock_loop_func, caplog):
    msg = make_mock_msg()
    msgs = [msg]
    run_loop, job_func, commit_func, msg_to_event_func, stop, result = mock_loop_func
    deserializer = get_fetch_deserializer()
    msg_to_event_func = MagicMock(
        wraps=lambda msg: avro_msg_to_event(msg.value(), deserializer)
    )

    with caplog.at_level("ERROR"):
        t = threading.Thread(
            target=lambda: run_loop(msgs, result, msg_to_event_func=msg_to_event_func),
            daemon=True,
        )
    t.start()

    time.sleep(0.5)
    stop.set()
    t.join(5)

    assert not t.is_alive()
    assert result["processed"] == 0
    assert any("Deserialization" in rec.getMessage() for rec in caplog.records)
    job_func.assert_not_called()
    commit_func.assert_called_once_with(msg)
    msg_to_event_func.assert_called_once_with(msg)


@pytest.mark.integration
def test_loop_poll_message_job_error(mock_loop_func, caplog):
    msg = make_mock_msg()
    msgs = [msg]
    run_loop, job_func, commit_func, msg_to_event_func, stop, result = mock_loop_func

    job_func = MagicMock(side_effect=Exception("boom"))

    with caplog.at_level("ERROR"):
        t = threading.Thread(
            target=lambda: run_loop(msgs, result, job_func=job_func), daemon=True
        )
    t.start()

    time.sleep(0.5)
    stop.set()
    t.join(5)

    assert not t.is_alive()
    assert result["processed"] == 0
    assert any("boom" in rec.getMessage() for rec in caplog.records)
    job_func.assert_called_once()
    commit_func.assert_not_called()
    msg_to_event_func.assert_called_once_with(msg)
