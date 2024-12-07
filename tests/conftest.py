import asyncio
import contextlib
from typing import AsyncGenerator, Callable, Annotated

import pytest
import pytest_asyncio
from fastapi import Depends
from furiousapi.api import ModelController
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

from tests.models import MyRepository


@pytest.fixture(scope="session", autouse=True)
def event_loop():
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(autouse=True)
def _create_table2() -> None:
    engine = create_engine(
        "sqlite:///test_db.sqlite",
        execution_options={"schema_translate_map": {None: "main"}},
        echo=False,
    )
    with contextlib.suppress(OperationalError):
        SQLModel.metadata.tables["my_model"].drop(bind=engine)

    SQLModel.metadata.tables["my_model"].create(bind=engine)


@pytest.fixture(scope="session")
def _create_table() -> None:
    engine = create_engine(
        "sqlite:///test_db_persistent.sqlite",
        execution_options={"schema_translate_map": {None: "main"}},
        echo=False,
    )
    with contextlib.suppress(OperationalError):
        SQLModel.metadata.tables["my_model"].drop(bind=engine)

    SQLModel.metadata.tables["my_model"].create(bind=engine)


@pytest.fixture
def async_sql_engine() -> AsyncEngine:
    return create_async_engine(
        "sqlite+aiosqlite:///test_db.sqlite",
        execution_options={"schema_translate_map": {None: "main"}},
        echo=False,
    )


@pytest.fixture(scope="session")
def async_persistent_sql_engine() -> AsyncEngine:
    return create_async_engine(
        "sqlite+aiosqlite:///test_db_persistent.sqlite",
        execution_options={"schema_translate_map": {None: "main"}},
        echo=False,
    )


@pytest_asyncio.fixture(scope="session")
async def sql_persistent_session(async_persistent_sql_engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSession(async_persistent_sql_engine, expire_on_commit=False) as session:
        yield session


@pytest_asyncio.fixture()
async def sql_session(async_sql_engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSession(async_sql_engine, expire_on_commit=False) as session:
        yield session


@pytest_asyncio.fixture()
async def my_repository_dep(_create_table: None, sql_session: AsyncSession) -> Callable[..., MyRepository]:
    def dep() -> MyRepository:
        return MyRepository(sql_session)

    return dep


@pytest_asyncio.fixture(scope="session")
async def my_repository(_create_table: None, sql_persistent_session: AsyncSession) -> MyRepository:
    return MyRepository(sql_persistent_session)


@pytest_asyncio.fixture()
def controller(my_repository_dep: MyRepository):
    class MyController(ModelController):
        repository: Annotated[MyRepository, Depends(my_repository_dep)]

    return MyController
