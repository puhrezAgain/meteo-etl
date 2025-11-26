import pytest
import pytest_asyncio
import os
import subprocess
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker
from etl import constants

@pytest.fixture(scope="session")
def migrated_db_url():
    url = os.environ.get("TEST_DATABASE_URL") or URL.create(
        drivername="postgresql+asyncpg",
        username=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ.get("DB_HOST", "localhost"),
        port=int(os.environ.get("DB_PORT", "5432")),
        database=os.environ["DB_NAME"],
    ).render_as_string(hide_password=False)
    subprocess.run(["python", "-m", "alembic", "upgrade", "head"], check=True)
    return url

@pytest_asyncio.fixture(scope="session")
async def async_engine(migrated_db_url):
    engine = create_async_engine(migrated_db_url, future=True)
    yield engine
    await engine.dispose()

@pytest_asyncio.fixture()
async def async_session(async_engine):
    async with async_engine.connect() as conn:
        trans = await conn.begin()
        AsyncSessionLocal = sessionmaker(
            bind=conn, class_=AsyncSession,
            expire_on_commit=False,
            future=True
        )
        async with AsyncSessionLocal() as session:
            try:
                yield session
            finally:
                await session.close()
                await trans.rollback()