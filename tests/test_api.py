import datetime
import logging
from http import HTTPStatus
from typing import TYPE_CHECKING

import pytest
from fastapi import FastAPI
from furiousapi.api import ModelController
from furiousapi.api.exception_handling import furious_db_exception_handler, furious_api_exception_handler
from furiousapi.api.exceptions import FuriousAPIError
from furiousapi.db.exceptions import FuriousEntityError
from furiousapi.pydantic import PYDANTIC_V2
from sqlmodel.ext.asyncio.session import AsyncSession
from starlette.testclient import TestClient

import tests.text_queries
from tests.models import MyModel, Foreign, MyModelCreate, Foo

if TYPE_CHECKING:
    from sqlmodel import SQLModel


@pytest.fixture
def app(controller: ModelController) -> FastAPI:
    app = FastAPI()
    app.include_router(controller.api_router, prefix="/model1")
    app.add_exception_handler(FuriousEntityError, furious_db_exception_handler)
    app.add_exception_handler(FuriousAPIError, furious_api_exception_handler)

    return app


@pytest.fixture
def test_client(app: FastAPI) -> TestClient:
    return TestClient(app)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    [
        "/model1",
    ],
)
async def test_create(
    test_client: "TestClient",
    path: str,
) -> None:
    model = MyModel(
        another_id=1,
        created_at=datetime.datetime(2023, 1, 1, 0, 1),
        int_number=1,
        float_number=1,
        is_boolean=True,
    )
    id_ = await create_model(model, path, test_client)
    response = test_client.get(path + f"/{id_}")
    json = response.json()
    if PYDANTIC_V2:
        actual = MyModel.model_validate(json)
    else:
        actual = MyModel.parse_obj(json)
    model.id = id_
    assert actual == model


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    [
        "/model1",
    ],
)
async def test_get(test_client: "TestClient", path: str) -> None:
    model = MyModel(
        another_id=1, created_at=datetime.datetime(2023, 1, 1, 0, 1), int_number=1, float_number=1, is_boolean=True
    )
    id_ = await create_model(model, path, test_client)
    response = test_client.get(f"{path}/{id_}")
    model.id = id_
    assert response.status_code == HTTPStatus.OK
    if PYDANTIC_V2:
        dump = model.model_dump(by_alias=True, mode="json")
    assert response.json() == dump


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    [
        "/model1",
    ],
)
async def test_list(test_client: "TestClient", path: str) -> None:
    model = MyModel(
        another_id=1, created_at=datetime.datetime(2023, 1, 1, 0, 1), int_number=1, float_number=1, is_boolean=True
    )
    id_ = await create_model(model, path, test_client)
    model.id = id_
    list_response = test_client.get(path)
    assert list_response.status_code == HTTPStatus.OK, list_response.json()
    if PYDANTIC_V2:
        assert model.model_validate(list_response.json()["items"][0]) == model
    else:
        assert model.parse_obj(list_response.json()["items"][0]) == model


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "param"),
    [
        ("/model1", "another_id"),
    ],
)
async def test_update(test_client: "TestClient", path: str, param: str) -> None:
    model = MyModel(
        another_id=1,
        created_at=datetime.datetime(2023, 1, 1, 0, 1),
        int_number=1,
        float_number=1,
        is_boolean=True,
    )
    id_ = await create_model(model, path, test_client)
    model.id = id_
    setattr(model, param, 100)
    if PYDANTIC_V2:
        d = model.model_dump(
            by_alias=True,
            mode="json",
            exclude_unset=True,
        )
        expected = model.model_dump(by_alias=True, mode="json")
    else:
        d = model.dict(by_alias=True, exclude_unset=True)
        expected = model.dict(by_alias=True)
    response = test_client.put(path + f"/{model.id}", json=d)
    assert response.status_code == HTTPStatus.OK
    assert response.json() == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    [
        "/model1",
    ],
)
async def test_delete(test_client: "TestClient", path: str) -> None:
    model = MyModel(
        another_id=1,
        created_at=datetime.datetime(2023, 1, 1, 0, 1),
        int_number=1,
        float_number=1,
        is_boolean=False,
    )
    model.id = await create_model(model, path, test_client)

    response = test_client.delete(f"{path}/{model.id}")
    assert response.status_code == HTTPStatus.OK

    response = test_client.get(f"{path}/{model.id}")
    assert response.status_code == HTTPStatus.NOT_FOUND


async def create_model(model: "SQLModel", path: str, test_client: "TestClient") -> int | None:
    if PYDANTIC_V2:
        dump = model.model_dump(by_alias=True, mode="json")
        create_response = test_client.post(path, json=dump)
    else:
        create_response = test_client.post(path, data=model.dict(by_alias=True))
    assert create_response.status_code == HTTPStatus.OK
    json = create_response.json()
    return json["id"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("rql", "sql"), [pytest.param(*x["params"], id=x["id"]) for x in tests.text_queries.ALL_TEST_CASES]
)
async def test_query(
    test_client: TestClient, sql_session: AsyncSession, caplog: pytest.LogCaptureFixture, rql: str, sql: str
) -> None:
    foreign1 = Foreign(id=1, name="aaa", foo=Foo(name="a123"))
    foreign2 = Foreign(id=2, name="bbb", foo=Foo(name="b123"))
    sql_session.add(foreign1)
    sql_session.add(foreign2)
    await sql_session.commit()
    await sql_session.refresh(foreign1)
    await sql_session.refresh(foreign2)
    model1 = MyModelCreate(
        another_id=1, int_number=1, float_number=2, is_boolean=True, foreign_id1=foreign1.id, foreign_id2=foreign2.id
    )
    await create_model(model1, "/model1", test_client)
    caplog.set_level(logging.DEBUG, logger="furiousapi.sqlmodel.query.transform")
    test_client.get(f"/model1?q={rql}")
    assert caplog.messages[0].strip() == sql.strip().replace("\t", "")
