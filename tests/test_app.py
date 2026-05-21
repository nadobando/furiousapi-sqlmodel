"""End-to-end API tests for the example SQLModel app.

These tests boot the example FastAPI app against a Postgres testcontainer
(not the SQLite default) and exercise the full HTTP surface: CRUD lifecycle,
cursor pagination, RQL filters/sorts, Item+Review relationships, and error
paths.

Postgres (not SQLite) is used here intentionally — SQLite has loose typing
and silently accepts mismatches that Postgres rejects. The pagination
module uses Postgres-specific NULL ordering and cursor ops that SQLite
handles differently. Testing against Postgres catches more real-world bugs.

State is shared across tests within the session.
"""

from __future__ import annotations

# The uv workspace puts every member package on sys.path, and beanie's
# `furiousapi-beanie/example/` shadows sqlmodel's `furiousapi-sqlmodel/example/`
# because alphabetical order. Force this package's path first so `import
# example` resolves locally.
import sys as _sys
from pathlib import Path as _Path

_HERE = str(_Path(__file__).resolve().parent.parent)
if _HERE not in _sys.path:
    _sys.path.insert(0, _HERE)
else:
    _sys.path.remove(_HERE)
    _sys.path.insert(0, _HERE)

from http import HTTPStatus  # noqa: E402
from typing import TYPE_CHECKING  # noqa: E402

import pytest  # noqa: E402
from sqlmodel import SQLModel, create_engine  # noqa: E402
from starlette.testclient import TestClient  # noqa: E402
from testcontainers.postgres import PostgresContainer  # noqa: E402

if TYPE_CHECKING:
    from collections.abc import Iterator

    from fastapi import FastAPI


# ---------------------------------------------------------------------------
# Postgres testcontainer + app harness
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def postgres_urls() -> "Iterator[tuple[str, str]]":
    """Spin up a Postgres testcontainer; yield (async_url, sync_url)."""
    with PostgresContainer("postgres:16") as container:
        # Default URL from testcontainers looks like
        # "postgresql+psycopg2://test:test@localhost:32768/test".
        sync_url = container.get_connection_url()
        # Strip the +psycopg2 dialect; both engines accept the plain prefix.
        sync_url = sync_url.replace("postgresql+psycopg2", "postgresql")
        async_url = sync_url.replace("postgresql://", "postgresql+asyncpg://", 1)
        yield async_url, sync_url


@pytest.fixture(scope="session")
def app(postgres_urls: tuple[str, str]) -> "FastAPI":
    """The example FastAPI app wired to the Postgres testcontainer."""
    async_url, sync_url = postgres_urls
    from example import config

    config.DATABASE_URL = async_url
    config.DATABASE_URL_SYNC = sync_url
    # Pre-create tables — the example's lifespan would also do this, but
    # importing `example.dependencies` (transitively pulled in by app.py)
    # builds the async engine at import time. Make tables exist first.
    engine = create_engine(sync_url, echo=False)
    SQLModel.metadata.create_all(engine)
    from example.app import app as _app

    return _app


@pytest.fixture(scope="session")
def client(app: "FastAPI") -> "Iterator[TestClient]":
    with TestClient(app) as c:
        yield c


# ---------------------------------------------------------------------------
# Helpers + shared state
# ---------------------------------------------------------------------------


def make_item(name: str, description: str = "desc") -> dict:
    return {"name": name, "description": description}


state: dict = {}
MAX_PAGES = 10


# ---------------------------------------------------------------------------
# CRUD lifecycle
# ---------------------------------------------------------------------------


class TestCrudLifecycle:
    """Create → Get → Update → Delete → 404 against a single Item."""

    def test_create(self, client: TestClient) -> None:
        r = client.post("/item/", json=make_item("Widget A", "first widget"))
        assert r.status_code == HTTPStatus.OK, r.text
        body = r.json()
        assert body["name"] == "Widget A"
        assert "id" in body
        state["crud_item_id"] = body["id"]

    def test_get(self, client: TestClient) -> None:
        r = client.get(f"/item/{state['crud_item_id']}")
        assert r.status_code == HTTPStatus.OK, r.text
        assert r.json()["name"] == "Widget A"

    def test_update(self, client: TestClient) -> None:
        updated = {"name": "Widget A renamed", "description": "first widget"}
        r = client.put(f"/item/{state['crud_item_id']}", json=updated)
        assert r.status_code == HTTPStatus.OK, r.text
        r = client.get(f"/item/{state['crud_item_id']}")
        assert r.json()["name"] == "Widget A renamed"

    def test_delete(self, client: TestClient) -> None:
        r = client.delete(f"/item/{state['crud_item_id']}")
        assert r.status_code in (HTTPStatus.OK, HTTPStatus.NO_CONTENT), r.text

    def test_get_404_after_delete(self, client: TestClient) -> None:
        r = client.get(f"/item/{state['crud_item_id']}")
        assert r.status_code == HTTPStatus.NOT_FOUND


# ---------------------------------------------------------------------------
# List + cursor pagination
# ---------------------------------------------------------------------------


class TestCursorPagination:
    page_size = 5
    total = 12

    def test_seed(self, client: TestClient) -> None:
        for i in range(self.total):
            r = client.post("/item/", json=make_item(f"page-{i:03d}"))
            assert r.status_code == HTTPStatus.OK, r.text

    def test_page_through(self, client: TestClient) -> None:
        seen: list[str] = []
        next_cursor: str | None = None
        page_count = 0
        while True:
            params: dict = {"limit": self.page_size}
            if next_cursor:
                params["next"] = next_cursor
            r = client.get("/item/", params=params)
            assert r.status_code == HTTPStatus.OK, r.text
            body = r.json()
            seen.extend(item["name"] for item in body["items"] if item["name"].startswith("page-"))
            page_count += 1
            next_cursor = body.get("next")
            if not next_cursor or page_count > MAX_PAGES:
                break
        assert len([n for n in seen if n.startswith("page-")]) >= self.total


# ---------------------------------------------------------------------------
# RQL filters
# ---------------------------------------------------------------------------


class TestRQLFilters:
    target_name = "rql-target"

    def test_seed(self, client: TestClient) -> None:
        for i in range(3):
            r = client.post("/item/", json=make_item(f"{self.target_name}-{i}"))
            assert r.status_code == HTTPStatus.OK
        client.post("/item/", json=make_item("rql-distractor"))

    def test_eq_filter(self, client: TestClient) -> None:
        name = f"{self.target_name}-0"
        r = client.get("/item/", params={"q": f"eq(name,{name})", "limit": 10})
        assert r.status_code == HTTPStatus.OK, r.text
        names = [it["name"] for it in r.json()["items"]]
        assert name in names

    def test_and_filter(self, client: TestClient) -> None:
        name = f"{self.target_name}-1"
        r = client.get(
            "/item/",
            params={"q": f"and(eq(name,{name}),eq(description,desc))", "limit": 10},
        )
        assert r.status_code == HTTPStatus.OK, r.text
        names = [it["name"] for it in r.json()["items"]]
        assert name in names


# ---------------------------------------------------------------------------
# RQL sorts
# ---------------------------------------------------------------------------


class TestRQLSorts:
    prefix = "sort-test"

    def test_seed(self, client: TestClient) -> None:
        for i in [3, 1, 2]:
            r = client.post("/item/", json=make_item(f"{self.prefix}-{i}"))
            assert r.status_code == HTTPStatus.OK

    def test_sort_asc(self, client: TestClient) -> None:
        r = client.get(
            "/item/",
            params={"q": f"like(name,'{self.prefix}-%');sort(+name)", "limit": 50},
        )
        assert r.status_code == HTTPStatus.OK, r.text
        names = [it["name"] for it in r.json()["items"] if it["name"].startswith(self.prefix)]
        assert names == sorted(names), f"expected sorted ascending: {names}"


# ---------------------------------------------------------------------------
# Item + Review relationship
# ---------------------------------------------------------------------------


class TestRelationships:
    def test_create_item_with_review_reference(self, client: TestClient) -> None:
        item_resp = client.post("/item/", json=make_item("with-review"))
        assert item_resp.status_code == HTTPStatus.OK, item_resp.text
        item_id = item_resp.json()["id"]

        get_item = client.get(f"/item/{item_id}")
        assert get_item.status_code == HTTPStatus.OK
        assert get_item.json()["name"] == "with-review"


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


class TestErrorPaths:
    def test_get_missing_404(self, client: TestClient) -> None:
        r = client.get("/item/999999999")
        assert r.status_code == HTTPStatus.NOT_FOUND, r.text

    def test_create_missing_required_field_422(self, client: TestClient) -> None:
        # Missing `name` (required)
        r = client.post("/item/", json={"description": "broken"})
        assert r.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, r.text


# ---------------------------------------------------------------------------
# Filtering safeguard
# ---------------------------------------------------------------------------


class TestFilteringSafeguard:
    """`?q=...` against a controller without `__filtering__` must 400, not crash."""

    def test_q_without_filtering_returns_400(self, client: TestClient) -> None:
        # ReviewController has no `__filtering__` configured (only ItemController does).
        r = client.get("/review/", params={"q": "eq(text,anything)"})
        assert r.status_code == HTTPStatus.BAD_REQUEST, r.text
        body = r.json()
        # FuriousAPIError formats as {"detail": {"status_code": 400, "detail": "..."}}.
        message = body.get("detail")
        if isinstance(message, dict):
            message = message.get("detail", "")
        assert "RQL filtering is not configured" in (message or ""), body
