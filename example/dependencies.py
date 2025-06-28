from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel.ext.asyncio.session import AsyncSession

from example.config import CONNECTION_STRING
from example.repositories import ItemRepository, ReviewRepository

engine = create_async_engine(
    f"sqlite+aiosqlite:///{CONNECTION_STRING}",
    execution_options={"schema_translate_map": {None: "main"}},
    echo=False,
)


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
