from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel.ext.asyncio.session import AsyncSession

from example import config
from example.repositories import ItemRepository, ReviewRepository

_engine_kwargs: dict = {"echo": False}
if config.DATABASE_URL.startswith("sqlite"):
    _engine_kwargs["execution_options"] = {"schema_translate_map": {None: "main"}}

engine = create_async_engine(config.DATABASE_URL, **_engine_kwargs)


async def sql_session() -> AsyncSession:
    async with AsyncSession(engine, expire_on_commit=False) as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(sql_session)]


def item_repository() -> ItemRepository:
    def dep(session: SessionDep) -> ItemRepository:
        return ItemRepository(session)

    return Depends(dep)


def review_repository() -> ReviewRepository:
    def dep(session: SessionDep) -> ReviewRepository:
        return ReviewRepository(session)

    return Depends(dep)
