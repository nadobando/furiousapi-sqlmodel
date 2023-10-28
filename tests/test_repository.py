from __future__ import annotations

import contextlib
import logging
from datetime import datetime
from typing import TYPE_CHECKING, List, Optional, Tuple

import pytest
import pytest_asyncio
from furiousapi.core.db.exceptions import EntityAlreadyExistsError
from furiousapi.core.pagination import CursorPaginationParams
from sqlalchemy import Column, DateTime
from sqlalchemy.exc import OperationalError

from furiousapi.sqlmodel.models import FuriousSQLModel
from furiousapi.sqlmodel.repository import BaseSQLRepository, TSQLModel
from sqlmodel import Field, SQLModel, create_engine
from tests.utils import get_first_doc_from_cache

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest
    from _pytest.logging import LogCaptureFixture
    from furiousapi.core.db.fields import SortableFieldEnum

    from sqlmodel.ext.asyncio.session import AsyncSession

PAGINATION = 5

CACHE_KEY = "sql_doc"


class MyModel(FuriousSQLModel, table=True):  # type: ignore[call-arg]
    __tablename__ = "my_model"

    id: Optional[int] = Field(nullable=False, primary_key=True)
    created_at: Optional[datetime] = Field(None, sa_column=Column(DateTime(timezone=True)))
    another_id: int
    int_number: int
    float_number: int
    is_boolean: bool
    nullable: Optional[int]


class MyRepository(BaseSQLRepository[MyModel]): ...


@pytest.fixture(scope="session")
def _create_table() -> None:
    engine = create_engine(
        "sqlite:///test_db.sqlite",
        execution_options={"schema_translate_map": {None: "main"}},
        echo=False,
    )
    with contextlib.suppress(OperationalError):
        SQLModel.metadata.tables["my_model"].drop(bind=engine)

    SQLModel.metadata.tables["my_model"].create(bind=engine)


@pytest_asyncio.fixture(scope="session")
async def my_repository(_create_table: None, sql_session: AsyncSession) -> MyRepository:
    return MyRepository(sql_session)


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
        )
        await my_repository.add(model)
        result.append(model.json())
    nullable_alternator = 0
    for i in range(PAGINATION):
        model = MyModel(
            created_at=datetime(2023, 1, 1, 0, i + 1),
            another_id=i + PAGINATION + 1,
            int_number=i + 1,
            float_number=(i % 4) + 1,
            is_boolean=False,
            nullable=nullable_alternator % 2 and 1 or None,
        )
        nullable_alternator += 1
        await my_repository.add(model)
        result.append(model.json())

    request.config.cache.set(CACHE_KEY, result)
    yield


@pytest.mark.parametrize("limit", [2, 5, 10], ids=["limit 2", "limit 5", "limit 10"])
@pytest.mark.parametrize(
    ("sorting", "filtering", "expected"),
    [
        pytest.param([], None, list(range(1, (PAGINATION * 2) + 1)), id="sort by id asc"),
        pytest.param([("id", "__neg__")], None, list(range((PAGINATION * 2), 0, -1)), id="sort by id desc"),
        pytest.param(
            [("another_id", "__pos__")],
            None,
            list(range(1, (PAGINATION * 2) + 1)),
            id="sort by another_id asc",
        ),
        pytest.param(
            [("another_id", "__neg__")],
            None,
            list(range((PAGINATION * 2), 0, -1)),
            id="sort by another_id desc",
        ),
        pytest.param([("int_number", "__pos__")], None, [1, 6, 2, 7, 3, 8, 4, 9, 5, 10], id="sort by int_number asc"),
        pytest.param([("int_number", "__neg__")], None, [10, 5, 9, 4, 8, 3, 7, 2, 6, 1], id="sort by int_number desc"),
        pytest.param(
            [("float_number", "__pos__")],
            None,
            [1, 5, 6, 10, 2, 7, 3, 8, 4, 9],
            id="sort by float_number asc",
        ),
        pytest.param(
            [("float_number", "__neg__")],
            None,
            [9, 4, 8, 3, 7, 2, 10, 6, 5, 1],
            id="sort by float_number desc",
        ),
        pytest.param([("created_at", "__pos__")], None, [1, 6, 2, 7, 3, 8, 4, 9, 5, 10], id="sort by created_at asc"),
        pytest.param([("created_at", "__neg__")], None, [10, 5, 9, 4, 8, 3, 7, 2, 6, 1], id="sort by created_at desc"),
        pytest.param([("is_boolean", "__pos__")], None, [6, 7, 8, 9, 10, 1, 2, 3, 4, 5], id="sort by is_boolean asc"),
        pytest.param([("is_boolean", "__neg__")], None, [5, 4, 3, 2, 1, 10, 9, 8, 7, 6], id="sort by is_boolean desc"),
        pytest.param(
            [
                ("float_number", "__pos__"),
                ("int_number", "__pos__"),
            ],
            None,
            [1, 6, 5, 10, 2, 7, 3, 8, 4, 9],
            id="sort by (float_number:asc,int_number:asc)",
        ),
        pytest.param(
            [
                ("float_number", "__pos__"),
                ("int_number", "__neg__"),
            ],
            None,
            [10, 5, 6, 1, 7, 2, 8, 3, 9, 4],
            id="sort by (float_number:asc,int_number:desc)",
        ),
        pytest.param(
            [
                ("float_number", "__neg__"),
                ("int_number", "__pos__"),
            ],
            None,
            [4, 9, 3, 8, 2, 7, 1, 6, 5, 10],
            id="sort by (float_number:desc,int_number:asc)",
        ),
        pytest.param(
            [
                ("float_number", "__neg__"),
                ("int_number", "__neg__"),
            ],
            None,
            [9, 4, 8, 3, 7, 2, 10, 5, 6, 1],
            id="sort by (float_number:desc,int_number:desc)",
        ),
        pytest.param(
            [
                ("float_number", "__pos__"),
                ("int_number", "__pos__"),
                ("is_boolean", "__pos__"),
            ],
            None,
            [6, 1, 10, 5, 7, 2, 8, 3, 9, 4],
            id="sort by (float_number:asc, int_number:asc, is_boolean:asc)",
        ),
        pytest.param(
            [
                ("float_number", "__pos__"),
                ("int_number", "__neg__"),
                ("is_boolean", "__pos__"),
            ],
            None,
            [10, 5, 6, 1, 7, 2, 8, 3, 9, 4],
            id="sort by (float_number:asc, int_number:desc, is_boolean:asc)",
        ),
        pytest.param(
            [
                ("float_number", "__pos__"),
                ("int_number", "__pos__"),
                ("is_boolean", "__neg__"),
            ],
            None,
            [1, 6, 5, 10, 2, 7, 3, 8, 4, 9],
            id="sort by (float_number:asc, int_number:asc, is_boolean:desc)",
        ),
        pytest.param(
            [
                ("float_number", "__neg__"),
                ("int_number", "__neg__"),
                ("is_boolean", "__pos__"),
            ],
            None,
            [9, 4, 8, 3, 7, 2, 10, 5, 6, 1],
            id="sort by (float_number:desc, int_number:desc, is_boolean:asc)",
        ),
        pytest.param(
            [
                ("float_number", "__neg__"),
                ("int_number", "__pos__"),
                ("is_boolean", "__neg__"),
            ],
            None,
            [4, 9, 3, 8, 2, 7, 1, 6, 5, 10],
            id="sort by (float_number:desc, int_number:asc, is_boolean:desc)",
        ),
        pytest.param(
            [
                ("float_number", "__pos__"),
                ("int_number", "__neg__"),
                ("is_boolean", "__neg__"),
            ],
            None,
            [5, 10, 1, 6, 2, 7, 3, 8, 4, 9],
            id="sort by (float_number:asc, int_number:desc, is_boolean:desc)",
        ),
        pytest.param(
            [
                ("float_number", "__neg__"),
                ("int_number", "__neg__"),
                ("is_boolean", "__neg__"),
            ],
            None,
            [4, 9, 3, 8, 2, 7, 5, 10, 1, 6],
            id="sort by (float_number:desc, int_number:desc, is_boolean:desc)",
        ),
        pytest.param(
            [],
            MyRepository.__filtering__(is_boolean=False),
            [6, 7, 8, 9, 10],
            id="sort by id asc and filter is_boolean False",
        ),
        pytest.param(
            [("id", "__neg__")],
            MyRepository.__filtering__(is_boolean=False),
            list(reversed([6, 7, 8, 9, 10])),
            id="sort by id desc and filter is_boolean False",
        ),
        pytest.param(
            [],
            MyRepository.__filtering__(is_boolean=False, float_number=1),
            [6, 10],
            id="sort by id asc and filter is_boolean=False and float_number=1",
        ),
        pytest.param(
            [("id", "__neg__")],
            MyRepository.__filtering__(is_boolean=False, float_number=1),
            list(reversed([6, 10])),
            id="sort by id desc and filter is_boolean=False and float_number=1",
        ),
    ],
)
@pytest.mark.asyncio()
async def test_list_with_sorting_and_filter(
    caplog: LogCaptureFixture,
    my_repository: MyRepository,
    limit: int,
    sorting: Tuple[str, str],
    filtering: TSQLModel,
    expected: list[int],
):
    caplog.set_level(logging.DEBUG, logger="furiousapi.db.sql.pagination")
    next_ = None
    result = []
    index_counter = 0
    while response := await my_repository.list(
        CursorPaginationParams(limit=limit, next=next_),
        sorting=[  # type: ignore[misc]
            getattr(getattr(my_repository.__sort__, field), op)() for field, op in sorting  # type: ignore[has-type]
        ],  # TODO: this list needs to be initialized here... unidentified bug
        filtering=filtering,
    ):
        result += [i.another_id for i in response.items]
        assert response.index == index_counter
        index_counter += limit
        if not response.next:
            break

        next_ = response.next

    assert result == expected


@pytest.mark.parametrize(
    "projection",
    [
        [MyRepository.__fields__.is_boolean],
        [MyRepository.__fields__.is_boolean, MyRepository.__fields__.float_number],
        [MyRepository.__fields__.is_boolean, MyRepository.__fields__.float_number, MyRepository.__fields__.int_number],
    ],
)
@pytest.mark.asyncio()
async def test_list_with_projection(my_repository: MyRepository, projection: List[SortableFieldEnum]):
    next_ = None

    while response := await my_repository.list(
        CursorPaginationParams(limit=10, next=next_),
        projection=projection,
    ):
        for _i, item in enumerate(response.items):
            item_dict = item.dict()
            item_dict.pop("id")
            # TODO:
            #   ```python
            #   ```
            #   currently from test suite the last row returns non projected
            #   this doesn't happens in the application
            for field in projection:
                assert field.value in item_dict

        if not response.next:
            break

        next_ = response.next


@pytest.mark.asyncio()
async def test_get(request: FixtureRequest, my_repository: MyRepository):
    first_doc = get_first_doc_from_cache(request, CACHE_KEY, MyModel)
    doc = await my_repository.get(first_doc.id)
    assert doc == first_doc


@pytest.mark.asyncio()
async def test_add__when__entity_already_exists__raises_entity_already_exists_error(
    request: FixtureRequest,
    my_repository: MyRepository,
):
    entity = get_first_doc_from_cache(request, CACHE_KEY, MyModel)
    with pytest.raises(EntityAlreadyExistsError):
        await my_repository.add(entity)
