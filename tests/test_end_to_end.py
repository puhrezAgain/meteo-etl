import pytest
from sqlalchemy import text
@pytest.mark.asyncio
async def test_session(async_session):
    r = await async_session.execute(text("SELECT 1"))
    assert r.scalar() == 1