import asyncio
import contextlib
import sys
import traceback
from datetime import datetime
from typing import AsyncGenerator, Callable, Annotated, TextIO

import pytest
import pytest_asyncio
from _pytest.fixtures import FixtureRequest
from fastapi import Depends
from fastapi.encoders import jsonable_encoder
from furiousapi.api import ModelController
from furiousapi.pydantic import PYDANTIC_V2
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

from furiousapi.sqlmodel.query.model import RQLModelSQL
from tests.models import MyRepository, MyModelCreate, MyModel, PAGINATION, CACHE_KEY, Foreign, Foo, ForeignRepository


@pytest.fixture(scope="session")
def event_loop():
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    yield loop
    loop.close()


class DebugStdout:
    def __init__(self, original_stdout: TextIO):
        self.original_stdout = original_stdout

    def write(self, message: str) -> None:
        if message.strip():  # Avoid processing empty or newline-only messages
            # Get the stack trace to locate where the print was called
            stack = traceback.extract_stack(limit=3)
            caller_info = stack[-2]  # Caller is the second-to-last in the stack
            location = f"{caller_info.filename}:{caller_info.lineno} in {caller_info.name}"
            self.original_stdout.write(f"[{location}] {message}")
        else:
            self.original_stdout.write(message)  # Preserve blank lines or newlines

    def flush(self):
        self.original_stdout.flush()


# Monkey-patch sys.stdout
sys.stdout = DebugStdout(sys.stdout)


@pytest.fixture(autouse=True)
def _create_table2() -> None:
    engine = create_engine(
        "sqlite:///test_db.sqlite",
        execution_options={"schema_translate_map": {None: "main"}},
    )
    with contextlib.suppress(OperationalError):
        SQLModel.metadata.drop_all(bind=engine)

    SQLModel.metadata.create_all(engine)


@pytest.fixture(scope="session", autouse=True)
def _create_table() -> None:
    engine = create_engine(
        "sqlite:///test_db_persistent.sqlite",
        execution_options={"schema_translate_map": {None: "main"}},
    )
    with contextlib.suppress(OperationalError):
        SQLModel.metadata.drop_all(bind=engine)

    SQLModel.metadata.create_all(bind=engine)


@pytest.fixture
def async_sql_engine() -> AsyncEngine:
    return create_async_engine(
        "sqlite+aiosqlite:///test_db.sqlite",
    )


@pytest.fixture(scope="session")
def async_persistent_sql_engine() -> AsyncEngine:
    return create_async_engine(
        "sqlite+aiosqlite:///test_db_persistent.sqlite",
        execution_options={"schema_translate_map": {None: "main"}},
    )


@pytest_asyncio.fixture(scope="session")
async def sql_persistent_session(async_persistent_sql_engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSession(async_persistent_sql_engine, expire_on_commit=False) as session:
        yield session
        session.expunge_all()


@pytest_asyncio.fixture()
async def sql_session(async_sql_engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSession(async_sql_engine, expire_on_commit=False) as session:
        yield session
        session.expunge_all()


@pytest_asyncio.fixture(scope="session", autouse=True)
async def init_data(my_repository: MyRepository, request: FixtureRequest):
    result = []
    for i in range(PAGINATION):
        model = MyModel(
            created_at=datetime(2023, 1, 1, 0, i + 1),
            another_id=i + 1,
            int_number=i + 1,
            float_number=(i % 4) + 1,
            is_boolean=True,
            foreign1=Foreign(name="aasd", foo=Foo(name="aaa")),
            foreign2=Foreign(name="aasd", foo=Foo(name="aaa")),
        )
        add_model = await my_repository.add(model)
        if add_model:
            if PYDANTIC_V2:
                js = add_model.model_dump(mode="json")
            else:
                js = jsonable_encoder(add_model)
            result.append(js)

    for nullable_alternator, i in enumerate(range(PAGINATION)):
        model = MyModel(
            created_at=datetime(2023, 1, 1, 0, i + 1),
            another_id=i + PAGINATION + 1,
            int_number=i + 1,
            float_number=(i % 4) + 1,
            is_boolean=False,
            nullable=((nullable_alternator - 1) % 2 and 1) or None,
            foreign1=Foreign(name="aaa", foo=Foo(name="ccc")),
            foreign2=Foreign(name="bbb", foo=Foo(name="ddd")),
        )
        add_model = await my_repository.add(model)
        if add_model:
            if PYDANTIC_V2:
                js = add_model.model_dump(mode="json")
            else:
                js = jsonable_encoder(add_model)
            result.append(js)

    request.config.cache.set(CACHE_KEY, result)
    my_repository.session.expunge_all()
    yield


@pytest_asyncio.fixture()
async def my_repository_dep(sql_session: AsyncSession) -> Callable[..., MyRepository]:
    def dep() -> MyRepository:
        return MyRepository(sql_session)

    return dep


@pytest_asyncio.fixture(scope="session")
async def my_repository(sql_persistent_session: AsyncSession) -> MyRepository:
    return MyRepository(sql_persistent_session)


@pytest_asyncio.fixture(scope="session")
async def foreign_repository(sql_persistent_session: AsyncSession) -> ForeignRepository:
    return ForeignRepository(sql_persistent_session)


@pytest_asyncio.fixture()
def controller(my_repository_dep: MyRepository):
    class MyModelRQL(RQLModelSQL):
        __model__ = MyModel

    class MyController(ModelController):
        repository: Annotated[MyRepository, Depends(my_repository_dep)]
        create_model = MyModelCreate
        __filtering__ = MyModelRQL

    return MyController
