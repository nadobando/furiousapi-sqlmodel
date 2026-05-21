from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from sqlmodel import SQLModel, create_engine

from example import config
from example.controllers import ItemController, ReviewController


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncGenerator[None, None]:
    kwargs = {"echo": False}
    if config.DATABASE_URL_SYNC.startswith("sqlite"):
        kwargs["execution_options"] = {"schema_translate_map": {None: "main"}}
    engine = create_engine(config.DATABASE_URL_SYNC, **kwargs)
    SQLModel.metadata.create_all(engine)
    yield
    # Add any cleanup code here if needed


app = FastAPI(lifespan=lifespan)

app.include_router(ItemController.api_router)
app.include_router(ReviewController.api_router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, port=8080)
