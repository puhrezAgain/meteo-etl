import pytest
from sqlalchemy import text
from etl.db import FetchMetadata
from etl.load import insert_new_fetch_metadata
class TestSanity:
    def test_session(self, db_session):
        r = db_session.execute(text("SELECT 1"))
        assert r.scalar() == 1
    def test_db_ready(self, db_session):
        assert db_session.query(FetchMetadata).count() == 0

class TestLoad:
    def test_insert_new_fetch_metadata(self, db_session):  
        inserted = insert_new_fetch_metadata("test", dict(test=True), db_session)
        tested = db_session.query(FetchMetadata).first()
        assert tested.id == inserted.id
