import pytest, uuid
from sqlalchemy import text, select, func
from sqlalchemy.exc import SQLAlchemyError
from etl.db import FetchMetadata, Observation, FetchStatus
from etl.load import (
    insert_fetch_metadata,
    update_fetch_metadata,
    load_observation_rows,
    LoadError,
)
from etl.models import FetchUpdate


class FakeSession:
    def execute(*args):
        raise SQLAlchemyError


class TestLoad:
    @pytest.mark.integration
    def test_insert_new_fetch_metadata(self, db_session):
        fetch_id = insert_fetch_metadata("test", dict(test=True), db_session)
        db_session.flush()
        tested = db_session.get(FetchMetadata, fetch_id)
        assert tested.request_params == dict(test=True)

    @pytest.mark.integration
    def test_update_fetch_metadata(self, db_session):
        fetch_id = insert_fetch_metadata("test", dict(test=True), db_session)
        db_session.flush()

        tested = db_session.get(FetchMetadata, fetch_id)
        assert not tested.error_data
        assert not tested.finished_at
        assert not tested.response_status
        assert tested.status == FetchStatus.PENDING

        update_fetch_metadata(
            FetchUpdate(
                fetch_id=fetch_id,
                response_status=200,
                error_data=dict(test=True),
                status=FetchStatus.SUCCESS,
            ),
            db_session,
        )
        db_session.flush()

        tested = db_session.get(FetchMetadata, fetch_id)
        assert tested.error_data == dict(test=True)
        assert tested.finished_at
        assert tested.response_status == 200
        assert tested.status == FetchStatus.SUCCESS

    def test_load_observation_rows(self, db_session, weather_records):
        fetch_id = insert_fetch_metadata("test", dict(test=True), db_session)
        db_session.flush()

        load_observation_rows(weather_records, fetch_id, db_session)
        count_query = (
            select(func.count())
            .select_from(Observation)
            .where(Observation.fetch_id == fetch_id)
        )
        row_count = db_session.execute(count_query).scalar_one()
        assert len(weather_records) == row_count

    @pytest.mark.unit
    def test_insert_fetch_metadata_errors(self):
        with pytest.raises(LoadError):
            insert_fetch_metadata("http://test", dict(), FakeSession())  # type: ignore[arg-type]

    @pytest.mark.unit
    def test_load_observation_rows_errors(self):
        with pytest.raises(LoadError):
            load_observation_rows([], uuid.uuid4(), FakeSession())  # type: ignore[arg-type]

    @pytest.mark.unit
    def test_update_fetch_metadata_errors(self):
        update = FetchUpdate(
            fetch_id=uuid.uuid4(),
            response_status=200,
            error_data=dict(test=True),
            status=FetchStatus.SUCCESS,
        )
        with pytest.raises(LoadError):
            update_fetch_metadata(update, FakeSession())  # type: ignore[arg-type]
