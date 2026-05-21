from contextlib import asynccontextmanager
from typing import AsyncGenerator

import uvicorn
from fastapi import FastAPI
from sqlmodel import SQLModel, create_engine

from example.config import CONNECTION_STRING
from example.controllers import ItemController, ReviewController


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncGenerator[None, None]:
    engine = create_engine(
        "sqlite:///" + CONNECTION_STRING,
        execution_options={"schema_translate_map": {None: "main"}},
        echo=False,
    )
    SQLModel.metadata.create_all(engine)
    yield
    # Add any cleanup code here if needed


app = FastAPI(lifespan=lifespan)

app.include_router(ItemController.api_router)
app.include_router(ReviewController.api_router)

if __name__ == "__main__":
    uvicorn.run(app, port=8080)
