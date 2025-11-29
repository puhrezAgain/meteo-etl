import pytest
import os
import json
from sqlalchemy import create_engine, event
from sqlalchemy_utils import database_exists, create_database, drop_database
from alembic.config import Config
from alembic import command
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker
from etl import models, config

TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL") or URL.create(
        drivername="postgresql+psycopg2",
        username=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ.get("DB_HOST", "localhost"),
        port=int(os.environ.get("DB_PORT", "5432")),
        database=f"{os.environ['DB_NAME']}_test" ,
    ).render_as_string(hide_password=False)

@pytest.fixture(scope="session")
def engine():
    if not database_exists(TEST_DATABASE_URL):
        create_database(TEST_DATABASE_URL)
    alembic_cfg = Config("alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url", TEST_DATABASE_URL)
    engine = create_engine(TEST_DATABASE_URL, future=True)

    try:
        alembic_cfg.attributes["connection"] = engine
        command.upgrade(alembic_cfg, "head")
    except Exception:
        # close connection if migrations fail to avoid leaks
        engine.dispose()
        raise

    yield engine
    engine.dispose()
    drop_database(TEST_DATABASE_URL)

@pytest.fixture()
def db_session(engine):
    conn = engine.connect()
    trans = conn.begin()
    SessionLocal = sessionmaker(
        bind=conn, expire_on_commit=False,
        future=True
    )
    session = SessionLocal()

    try:
        yield session
    finally:
        session.close()
        trans.rollback()
        conn.close()

@pytest.fixture(autouse=True)
def override_engine(monkeypatch, engine, db_session):
    from etl import db
    monkeypatch.setattr(db, "engine", engine)
    monkeypatch.setattr(db, "SessionLocal", lambda: db_session)

@pytest.fixture()
def weather_records():
    with open("tests/fixtures/meteo-data.json") as f:
        records = json.load(f)

    return [
        models.WeatherRecord.model_validate_json(record) for record in records
    ]