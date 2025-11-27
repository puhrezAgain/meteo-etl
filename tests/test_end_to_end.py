import pytest
from sqlalchemy import text, select, func
from etl.db import FetchMetadata, Observation
from etl.models import WeatherRecord
from etl.load import insert_new_fetch_metadata, update_fetch_metadata, load_observation_rows
class TestSanity:
    def test_session(self, db_session):
        r = db_session.execute(text("SELECT 1"))
        assert r.scalar() == 1
    def test_db_ready(self, db_session):
        assert db_session.query(FetchMetadata).count() == 0

    def test_weather_records(self, weather_records):
        assert isinstance(weather_records, list)
        assert all(isinstance(i, WeatherRecord) for i in weather_records)

class TestLoad:
    def test_insert_new_fetch_metadata(self, db_session):  
        inserted = insert_new_fetch_metadata("test", dict(test=True), db_session)
        tested = db_session.get(FetchMetadata, inserted.id)
        assert tested.request_params == dict(test=True)

    def test_update_fetch_metadata(self, db_session):
        inserted = insert_new_fetch_metadata("test", dict(test=True), db_session)
        assert not inserted.response_data 
        assert not inserted.response_status
        assert inserted.status == "pending"
        inserted = update_fetch_metadata(inserted, db_session, 200, dict(test=True))
        tested = db_session.get(FetchMetadata, inserted.id)
        assert tested.response_data == dict(test=True)
        assert tested.response_status == 200
    
    def test_load_observation_rows(self, db_session, weather_records):
        inserted = insert_new_fetch_metadata("test", dict(test=True), db_session)
        load_observation_rows(weather_records, db_session, inserted)
        count_query = select(func.count()).select_from(Observation).where(
            Observation.fetch_id == inserted.id
        )
        row_count = db_session.execute(count_query).scalar_one()
        assert len(weather_records) == row_count
    
        



