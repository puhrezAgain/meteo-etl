import pytest, os, json, time, uuid
from sqlalchemy import create_engine, event
from sqlalchemy_utils import database_exists, create_database, drop_database
from alembic.config import Config
from alembic import command
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroConsumer
from etl import models, config
from streaming.config import settings

TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL") or URL.create(
    drivername="postgresql+psycopg2",
    username=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
    host=os.environ.get("DB_HOST", "localhost"),
    port=int(os.environ.get("DB_PORT", "5432")),
    database=f"{os.environ['DB_NAME']}_test",
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
def db_session_maker(monkeypatch, engine):
    conn = engine.connect()
    trans = conn.begin()
    SessionLocal = sessionmaker(bind=conn, expire_on_commit=False, future=True)
    from etl import db

    monkeypatch.setattr(db, "engine", engine)
    monkeypatch.setattr(db, "SessionLocal", SessionLocal)

    try:
        yield SessionLocal
    finally:
        trans.rollback()
        conn.close()


@pytest.fixture(autouse=True)
def db_session(db_session_maker):
    session = db_session_maker()

    try:
        yield session
    finally:
        session.close()


@pytest.fixture()
def weather_records(monkeypatch):
    with open("tests/fixtures/meteo-data.json") as f:
        records = json.load(f)

    return [models.WeatherRecord.model_validate_json(record) for record in records]


@pytest.fixture
def meteo_payload():
    with open("tests/fixtures/meteo-payload.json") as f:
        return json.load(f)


@pytest.fixture
def override_meteo_api(monkeypatch, meteo_payload):
    monkeypatch.setattr(
        "etl.sources.run_extractor", lambda *args, **kwargs: meteo_payload
    )


@pytest.fixture
def temp_lake_dir(monkeypatch, tmp_path):
    from streaming.load import settings

    monkeypatch.setattr(settings, "RAW_DATA_DIR", tmp_path)
    return tmp_path


def wait_for_kafka():
    admin = AdminClient({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})
    start = time.time()

    while True:
        try:  
            admin.list_topics(timeout=5)
            return admin
        except Exception:
            if time.time() - start > 30:
                raise RuntimeError("Kafka did not become ready in time")
            time.sleep(1)

def create_topic(admin: AdminClient, name: str):
    new_topic = NewTopic(topic=name, num_partitions=1, replication_factor=1)
    fs = admin.create_topics([new_topic])
    try:
        fs[name].result
    except Exception as e:
        if "TopicExistsException" not in repr(e) and "TopicAlreadyExists" not in repr(e):
            raise


@pytest.fixture(scope="session")
def kafka_admin():
    return wait_for_kafka()

@pytest.fixture(scope="session")
def meteo_topic(kafka_admin) -> str:
    new_topic = settings.FETCH_TOPIC + "_test"
    create_topic(kafka_admin, new_topic)
    return new_topic


@pytest.fixture
def avro_consumer(meteo_topic):
    consumer = AvroConsumer({
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        "schema.registry.url": settings.SCHEMA_REGISTRY_URL,
        "group.id": f"pytest-meteo-{uuid.uuid4()}",
        "auto.offset.reset": "earliest"
    })

    consumer.subscribe([meteo_topic])
    try:
        yield consumer
    finally:
        consumer.close()
