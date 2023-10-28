import asyncio
from typing import AsyncGenerator

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from sqlmodel.ext.asyncio.session import AsyncSession


@pytest.fixture(scope="session", autouse=True)
def event_loop():
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def async_sql_engine() -> AsyncEngine:
    return create_async_engine(
        "sqlite+aiosqlite:///test_db.sqlite",
        execution_options={"schema_translate_map": {None: "main"}},
        echo=False,
    )


@pytest_asyncio.fixture(scope="session")
async def sql_session(async_sql_engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSession(async_sql_engine, expire_on_commit=False) as session:
        yield session
