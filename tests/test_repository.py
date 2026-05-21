from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Tuple

import pytest

from furiousapi.db import EntityAlreadyExistsError
from sqlalchemy import desc
from sqlmodel import select, asc

from tests.models import MyRepository, MyModel, PAGINATION, CACHE_KEY
from tests.utils import get_first_doc_from_cache

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest
    from _pytest.logging import LogCaptureFixture
    from furiousapi.sqlmodel.repository import TSQLModel
    from furiousapi.api.pagination import PaginatedResponse


@pytest.mark.parametrize("limit", [2, 5, 10], ids=["limit 2", "limit 5", "limit 10"])
@pytest.mark.parametrize(
    ("sorting", "filtering", "expected"),
    [
        pytest.param([], None, list(range(1, (PAGINATION * 2) + 1)), id="sort by id asc"),
        pytest.param([desc(MyModel.id)], None, list(range((PAGINATION * 2), 0, -1)), id="sort by id desc"),
        pytest.param(
            [MyModel.another_id.asc()],
            None,
            list(range(1, (PAGINATION * 2) + 1)),
            id="sort by another_id asc",
        ),
        pytest.param(
            [MyModel.another_id.desc()],
            None,
            list(range((PAGINATION * 2), 0, -1)),
            id="sort by another_id desc",
        ),
        pytest.param([MyModel.int_number.asc()], None, [1, 6, 2, 7, 3, 8, 4, 9, 5, 10], id="sort by int_number asc"),
        pytest.param([MyModel.int_number.desc()], None, [10, 5, 9, 4, 8, 3, 7, 2, 6, 1], id="sort by int_number desc"),
        pytest.param(
            [MyModel.float_number.asc()],
            None,
            [1, 5, 6, 10, 2, 7, 3, 8, 4, 9],
            id="sort by float_number asc",
        ),
        pytest.param(
            [MyModel.float_number.desc()],
            None,
            [9, 4, 8, 3, 7, 2, 10, 6, 5, 1],
            id="sort by float_number desc",
        ),
        pytest.param([asc(MyModel.created_at)], None, [1, 6, 2, 7, 3, 8, 4, 9, 5, 10], id="sort by created_at asc"),
        pytest.param([desc(MyModel.created_at)], None, [10, 5, 9, 4, 8, 3, 7, 2, 6, 1], id="sort by created_at desc"),
        pytest.param([asc(MyModel.is_boolean)], None, [6, 7, 8, 9, 10, 1, 2, 3, 4, 5], id="sort by is_boolean asc"),
        pytest.param([desc(MyModel.is_boolean)], None, [5, 4, 3, 2, 1, 10, 9, 8, 7, 6], id="sort by is_boolean desc"),
        pytest.param(
            [MyModel.float_number.asc(), MyModel.int_number.asc()],
            None,
            [1, 6, 5, 10, 2, 7, 3, 8, 4, 9],
            id="sort by (float_number:asc,int_number:asc)",
        ),
        pytest.param(
            [MyModel.float_number.asc(), MyModel.int_number.desc()],
            None,
            [10, 5, 6, 1, 7, 2, 8, 3, 9, 4],
            id="sort by (float_number:asc,int_number:desc)",
        ),
        pytest.param(
            [MyModel.float_number.desc(), MyModel.int_number.asc()],
            None,
            [4, 9, 3, 8, 2, 7, 1, 6, 5, 10],
            id="sort by (float_number:desc,int_number:asc)",
        ),
        pytest.param(
            [MyModel.float_number.desc(), MyModel.int_number.desc()],
            None,
            [9, 4, 8, 3, 7, 2, 10, 5, 6, 1],
            id="sort by (float_number:desc,int_number:desc)",
        ),
        pytest.param(
            [MyModel.float_number.asc(), MyModel.int_number.asc(), MyModel.is_boolean.asc()],
            None,
            [6, 1, 10, 5, 7, 2, 8, 3, 9, 4],
            id="sort by (float_number:asc, int_number:asc, is_boolean:asc)",
        ),
        pytest.param(
            [MyModel.float_number.asc(), MyModel.int_number.desc(), MyModel.is_boolean.asc()],
            None,
            [10, 5, 6, 1, 7, 2, 8, 3, 9, 4],
            id="sort by (float_number:asc, int_number:desc, is_boolean:asc)",
        ),
        pytest.param(
            [MyModel.float_number.asc(), MyModel.int_number.asc(), MyModel.is_boolean.desc()],
            None,
            [1, 6, 5, 10, 2, 7, 3, 8, 4, 9],
            id="sort by (float_number:asc, int_number:asc, is_boolean:desc)",
        ),
        pytest.param(
            [MyModel.float_number.desc(), MyModel.int_number.desc(), MyModel.is_boolean.asc()],
            None,
            [9, 4, 8, 3, 7, 2, 10, 5, 6, 1],
            id="sort by (float_number:desc, int_number:desc, is_boolean:asc)",
        ),
        pytest.param(
            [MyModel.float_number.desc(), MyModel.int_number.asc(), MyModel.is_boolean.desc()],
            None,
            [4, 9, 3, 8, 2, 7, 1, 6, 5, 10],
            id="sort by (float_number:desc, int_number:asc, is_boolean:desc)",
        ),
        pytest.param(
            [MyModel.float_number.asc(), MyModel.int_number.desc(), MyModel.is_boolean.desc()],
            None,
            [5, 10, 1, 6, 2, 7, 3, 8, 4, 9],
            id="sort by (float_number:asc, int_number:desc, is_boolean:desc)",
        ),
        pytest.param(
            [MyModel.float_number.desc(), MyModel.int_number.desc(), MyModel.is_boolean.desc()],
            None,
            [4, 9, 3, 8, 2, 7, 5, 10, 1, 6],
            id="sort by (float_number:desc, int_number:desc, is_boolean:desc)",
        ),
        pytest.param(
            [],
            MyModel.is_boolean == False,
            [6, 7, 8, 9, 10],
            id="sort by id asc and filter is_boolean False",
        ),
        pytest.param(
            [desc(MyModel.id)],
            MyModel.is_boolean == False,
            list(reversed([6, 7, 8, 9, 10])),
            id="sort by id desc and filter is_boolean False",
        ),
        pytest.param(
            [asc(MyModel.id)],
            (MyModel.is_boolean == False) & (MyModel.float_number == 1),
            [6, 10],
            id="sort by id asc and filter is_boolean=False and float_number=1",
        ),
        pytest.param(
            [desc(MyModel.id)],
            (MyModel.is_boolean == False) & (MyModel.float_number == 1),
            list(reversed([6, 10])),
            id="sort by id desc and filter is_boolean=False and float_number=1",
        ),
    ],
)
@pytest.mark.asyncio
async def test_list_with_sorting_and_filter(
    caplog: LogCaptureFixture,
    my_repository: MyRepository,
    limit: int,
    sorting: Tuple[str, str],
    filtering: TSQLModel,
    expected: list[int],
):
    caplog.set_level(logging.DEBUG)
    next_ = None
    result = []
    index_counter = 0
    query = select(MyModel)
    if filtering is not None:
        query = query.where(filtering)
    if sorting:
        query = query.order_by(*sorting)
    response: PaginatedResponse
    paginator = my_repository.get_paginator("cursor")
    while response := await paginator.get_page(query, limit, next_):
        result += [i.another_id for i in response.items]
        assert response.index == index_counter
        index_counter += limit
        if not response.next:
            break

        next_ = response.next

    assert result == expected


@pytest.mark.asyncio
async def test_get(request: FixtureRequest, my_repository: MyRepository):
    first_doc = get_first_doc_from_cache(request, CACHE_KEY, MyModel)
    doc = await my_repository.get(first_doc.id)
    assert doc == first_doc


@pytest.mark.asyncio
async def test_add__when__entity_already_exists__raises_entity_already_exists_error(
    request: FixtureRequest,
    my_repository: MyRepository,
):
    entity = get_first_doc_from_cache(request, CACHE_KEY, MyModel)
    with pytest.raises(EntityAlreadyExistsError):
        await my_repository.add(entity)
