import datetime
from http import HTTPStatus
from typing import TYPE_CHECKING

import pytest
from fastapi import FastAPI
from furiousapi.api import ModelController
from furiousapi.api.exception_handling import furious_db_exception_handler, furious_api_exception_handler
from furiousapi.api.exceptions import FuriousAPIError
from furiousapi.db.exceptions import FuriousEntityError
from furiousapi.pydantic import PYDANTIC_V2

from starlette.testclient import TestClient

from tests.models import MyModel

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
    await create_model(model, path, test_client)
    response = test_client.get(path + f"/{model.id}")
    json = response.json()

    actual = MyModel.parse_obj(json)
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
    await create_model(model, path, test_client)
    response = test_client.get(f"{path}/{model.id}")

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
    await create_model(model, path, test_client)
    list_response = test_client.get(path)
    assert list_response.status_code == HTTPStatus.OK, list_response.json()
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
    await create_model(model, path, test_client)
    assert model.id
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
    await create_model(model, path, test_client)

    response = test_client.delete(f"{path}/{model.id}")
    assert response.status_code == HTTPStatus.OK

    response = test_client.get(f"{path}/{model.id}")
    assert response.status_code == HTTPStatus.NOT_FOUND


async def create_model(model: "SQLModel", path: str, test_client: "TestClient") -> None:
    if PYDANTIC_V2:
        create_response = test_client.post(path, json=model.model_dump(by_alias=True, mode="json"))
    else:
        create_response = test_client.post(path, data=model.dict(by_alias=True))
    assert create_response.status_code == HTTPStatus.OK
    json = create_response.json()
    model.id = json["id"]
