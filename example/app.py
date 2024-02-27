from typing import Annotated

import uvicorn
from fastapi import FastAPI, Depends
from sqlalchemy.ext.asyncio import create_async_engine

from sqlmodel import SQLModel, Field,create_engine
from sqlmodel.ext.asyncio.session import AsyncSession

from furiousapi.api import ModelController
from furiousapi.sqlmodel import SQLRepository

app = FastAPI()


class Item(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    name: str
    description: str = Field(default=None, nullable=True)


class ItemRepository(SQLRepository[Item]):
    pass


engine = create_async_engine(
    "sqlite+aiosqlite:///test_db.sqlite",
    execution_options={"schema_translate_map": {None: "main"}},
    echo=False,
)


async def sql_session() -> AsyncSession:
    async with AsyncSession(engine, expire_on_commit=False) as session:

        yield session


SessionDep = Annotated[AsyncSession, Depends(sql_session)]


def repository() -> Depends:
    def dep(session: SessionDep) -> ItemRepository:
        return ItemRepository(session)

    return Depends(dep)


class ItemController(ModelController, prefix="/item", tags=["Items"]):
    repository = repository()

@app.on_event("startup")
async def startup():
    engine = create_engine(
        "sqlite:///test_db.sqlite",
        execution_options={"schema_translate_map": {None: "main"}},
        echo=False,
    )
    SQLModel.metadata.create_all(engine)

app.include_router(ItemController.api_router)

if __name__ == "__main__":
    uvicorn.run(app)
