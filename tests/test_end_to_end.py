import pytest
from sqlalchemy import text, select, func
from etl.db import FetchMetadata, Observation
from etl.models import WeatherRecord
from etl.load import insert_fetch_metadata, update_fetch_metadata, load_observation_rows
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
        fetch_id = insert_fetch_metadata("test", dict(test=True), db_session)
        db_session.flush()
        tested = db_session.get(FetchMetadata, fetch_id)
        assert tested.request_params == dict(test=True)

    def test_update_fetch_metadata(self, db_session):
        fetch_id = insert_fetch_metadata("test", dict(test=True), db_session)
        db_session.flush()

        tested = db_session.get(FetchMetadata, fetch_id)    
        assert not tested.response_data 
        assert not tested.response_status
        assert tested.status == "pending"
        
        update_fetch_metadata(fetch_id, 200, dict(test=True), "finished", db_session)
        db_session.flush()
        
        tested = db_session.get(FetchMetadata, fetch_id)
        assert tested.response_data == dict(test=True)
        assert tested.response_status == 200
        assert tested.status == "finished"
    
    def test_load_observation_rows(self, db_session, weather_records):
        fetch_id = insert_fetch_metadata("test", dict(test=True), db_session)
        db_session.flush()
        
        load_observation_rows(weather_records, fetch_id, db_session)
        count_query = select(func.count()).select_from(Observation).where(
            Observation.fetch_id == fetch_id
        )
        row_count = db_session.execute(count_query).scalar_one()
        assert len(weather_records) == row_count
    
        



