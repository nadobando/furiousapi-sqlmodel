from datetime import datetime
from typing import Optional, List

from furiousapi.pydantic import PYDANTIC_V2
from pydantic import ConfigDict, BaseModel
from sqlalchemy import Column, DateTime
from sqlmodel import Field, Relationship
from sqlmodel import SQLModel

from furiousapi.sqlmodel.repository import BaseSQLRepository

PAGINATION = 5
CACHE_KEY = "sql_doc"


class Foo(SQLModel, table=True):  # type: ignore[call-arg]
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    foreign: Optional["Foreign"] = Relationship(back_populates="foo", sa_relationship_kwargs={"uselist": False})


class BaseForeign(SQLModel):
    name: str = Field(index=True)
    foo_id: Optional[int] = Field(default=None, foreign_key="foo.id")


class Foreign(BaseForeign, table=True):  # type: ignore[call-arg]
    id: Optional[int] = Field(default=None, primary_key=True)
    foo: Optional["Foo"] = Relationship(back_populates="foreign", sa_relationship_kwargs={"uselist": False})

    model_config = ConfigDict(from_attributes=True)


class OneToManyModel(SQLModel, table=True):  # type: ignore[call-arg]
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    my_model_id: int = Field(foreign_key="my_model.id")


class ManyToManyModel(SQLModel, table=True):  # type: ignore[call-arg]
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str


class ManyToManyLinkModel(SQLModel, table=True):  # type: ignore[call-arg]
    many_id: Optional[int] = Field(default=None, primary_key=True, foreign_key="manytomanymodel.id")
    model_id: Optional[int] = Field(default=None, primary_key=True, foreign_key="my_model.id")


class BaseMyModel(BaseModel):
    created_at: Optional[datetime] = Field(None, sa_column=Column(DateTime(timezone=True)))
    another_id: int
    int_number: int
    float_number: int
    is_boolean: bool
    nullable: Optional[int] = None


class MyModelBase(BaseMyModel, SQLModel):
    foreign_id1: Optional[int] = Field(
        default=None,
        foreign_key="foreign.id",
    )
    foreign_id2: Optional[int] = Field(
        default=None,
        foreign_key="foreign.id",
    )


class MyModel(MyModelBase, table=True):  # type: ignore[call-arg]
    __tablename__ = "my_model"
    id: Optional[int] = Field(default=None, nullable=False, primary_key=True)
    parent_id: Optional[int] = Field(default=None, foreign_key="my_model.id")
    children: List["MyModel"] = Relationship(
        sa_relationship_kwargs={"cascade": "all", "foreign_keys": "[MyModel.parent_id]"}
    )
    foreign1: Optional["Foreign"] = Relationship(sa_relationship_kwargs={"foreign_keys": "[MyModel.foreign_id1]"})
    foreign2: Optional["Foreign"] = Relationship(sa_relationship_kwargs={"foreign_keys": "[MyModel.foreign_id2]"})
    one_to_many: Optional[List["OneToManyModel"]] = Relationship(
        sa_relationship_kwargs={"foreign_keys": "[OneToManyModel.my_model_id]"}
    )
    many_to_many: Optional[List[ManyToManyModel]] = Relationship(link_model=ManyToManyLinkModel)
    if PYDANTIC_V2:
        model_config = ConfigDict(validate_assignment=True, from_attributes=True)
    else:
        pass


class ForeignCreate(BaseForeign):
    foo: Optional[Foo]


class MyModelCreate(MyModelBase):
    foreign1: Optional[ForeignCreate] = None
    foreign2: Optional[ForeignCreate] = None


class MyRepository(BaseSQLRepository[MyModel]): ...


class ForeignRepository(BaseSQLRepository[Foreign]): ...
