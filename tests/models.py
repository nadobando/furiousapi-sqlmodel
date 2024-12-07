from __future__ import annotations

from typing import Optional

from furiousapi.pydantic import PYDANTIC_V2
from pydantic import ConfigDict
from sqlalchemy import Column, DateTime
from sqlmodel import Field
from sqlmodel import SQLModel

from furiousapi.sqlmodel.repository import BaseSQLRepository


from datetime import datetime  # noqa: TC003

PAGINATION = 5
CACHE_KEY = "sql_doc"


class MyModel(SQLModel, table=True):  # type: ignore[call-arg]
    __tablename__ = "my_model"

    id: Optional[int] = Field(default=None, nullable=False, primary_key=True)
    created_at: Optional[datetime] = Field(None, sa_column=Column(DateTime(timezone=True)))
    another_id: int
    int_number: int
    float_number: int
    is_boolean: bool
    nullable: Optional[int] = None

    if PYDANTIC_V2:
        model_config = ConfigDict(validate_assignment=True)
    else:
        pass


class MyRepository(BaseSQLRepository[MyModel]): ...


# bug: only needed with tox
if PYDANTIC_V2:
    MyRepository.__filtering__.model_rebuild()
