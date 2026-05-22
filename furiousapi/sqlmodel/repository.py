from __future__ import annotations

from collections import deque
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    Any,
    TypeVar,
    cast,
    overload,
    Literal,
)
from collections.abc import Iterable

from fastapi._compat import PYDANTIC_V2
from furiousapi.api.pagination import PaginationStrategyEnum
from furiousapi.db import BaseRepository
from furiousapi.db import EntityAlreadyExistsError, EntityNotFoundError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import load_only
from sqlmodel import SQLModel, select

from furiousapi.sqlmodel.pagination import SQLModelCursorPagination, SQLModelOffsetPagination
from furiousapi.sqlmodel.utils import collect_relationships

if TYPE_CHECKING:
    from sqlalchemy import Column, BinaryExpression
    from collections.abc import Iterable
    from sqlmodel.sql._expression_select_cls import SelectOfScalar, Select
    from furiousapi.core.types import TModelFields
    from sqlmodel.ext.asyncio.session import AsyncSession

TSQLModel = TypeVar("TSQLModel", bound=SQLModel)


def _get_model_fields(model: type[TSQLModel], fields: Iterable[TModelFields]) -> Iterable[str]:
    return (getattr(model, f.value) for f in fields)


class BaseSQLRepository(BaseRepository[TSQLModel]):
    __model__: type[TSQLModel]

    def __init_paginators__(self) -> None:
        self.__paginators__[PaginationStrategyEnum.CURSOR] = SQLModelCursorPagination(
            self.__primary_keys__, self.session, self.__model__
        )
        self.__paginators__[PaginationStrategyEnum.OFFSET] = SQLModelOffsetPagination(self.session)

    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        super().__init__()

    @cached_property
    def __primary_keys__(self) -> set[str]:
        # __table__ is attached by SQLAlchemy on table=True models, not declared
        # on the SQLModel stub.
        columns: list[Column] = self.__model__.__table__.primary_key.columns  # type: ignore[attr-defined]
        return {column.name for column in columns}

    def __primary_values(self, instance: TSQLModel) -> tuple:
        return tuple(getattr(instance, i) for i in self.__primary_keys__)

    @overload
    async def get(
        self,
        identifiers: int | str | dict[str, Any] | tuple[Any],
        fields: Iterable[TModelFields] | None = None,
        *,
        should_error: Literal[True] = True,
        options: list | None = None,
    ) -> TSQLModel: ...

    @overload
    async def get(
        self,
        identifiers: int | str | dict[str, Any] | tuple[Any],
        fields: Iterable[TModelFields] | None = None,
        *,
        options: list | None = None,
        should_error: Literal[False] = False,
    ) -> TSQLModel | None: ...

    async def get(
        self,
        identifiers: int | str | dict[str, Any] | tuple[Any],
        fields: Iterable[TModelFields] | None = None,
        *,
        should_error: bool = True,
        options: list | None = None,
    ) -> TSQLModel | None:
        options = options or []

        if fields:
            fields: Iterable[str] = _get_model_fields(self.__model__, fields)
            options.append(load_only(*fields))

        record = await self.session.get(self.__model__, identifiers, options=options)

        if not record and should_error:
            raise EntityNotFoundError(self.__model__, identifiers)

        return record

    async def add(self, entity: TSQLModel, *, commit: bool = True) -> TSQLModel | None:
        d = entity.model_dump(exclude=set(self.__model__.__sqlmodel_relationships__.keys()))
        rels = collect_relationships(self.__model__, entity)
        if PYDANTIC_V2:
            self.__model__.model_validate(entity.model_dump(mode="json"))
        else:
            self.__model__.parse_obj(entity.dict())
        entity = self.__model__(**d, **rels)

        self.session.add(entity)

        if commit:
            try:
                await self.session.commit()
                await self.session.refresh(entity)
            except IntegrityError as exc:
                await self.session.rollback()
                raise EntityAlreadyExistsError(self.__model__, self.__primary_values(entity)) from exc
            else:
                return entity

        return None

    async def patch(self, identifiers: Any, partial: TSQLModel, *, commit: bool = True) -> TSQLModel | None:
        """Partial update — only fields explicitly set on `partial` are written."""
        response = await self.get(identifiers)
        for key, value in partial.model_dump(exclude_unset=True, exclude=set(self.__primary_keys__)).items():
            setattr(response, key, value)
        self.session.add(response)
        if commit:
            await self.session.commit()
            await self.session.refresh(response)
            return response
        return None

    async def replace(self, identifiers: Any, entity: TSQLModel, *, commit: bool = True) -> TSQLModel | None:
        """Full replacement — every field on the entity is written, defaults included."""
        response = await self.get(identifiers)
        for key, value in entity.model_dump(exclude=set(self.__primary_keys__)).items():
            setattr(response, key, value)
        self.session.add(response)
        if commit:
            await self.session.commit()
            await self.session.refresh(response)
            return response
        return None

    async def delete(self, identifiers: tuple, *, commit: bool = True, **kwargs) -> None:
        response = await self.get(identifiers)
        await self.session.delete(response)
        if commit:
            await self.session.commit()

    async def bulk_create(self, bulk: list[TSQLModel], *, commit: bool = True) -> None:
        deque(map(self.session.add, bulk))
        if commit:
            return await self.session.commit()
        return None

    async def bulk_delete(self, bulk: list[TSQLModel | Any], *, commit: bool = True) -> None:
        deque(map(self.session.delete, bulk))
        if commit:
            await self.session.commit()

    async def bulk_update(self, bulk: list[TSQLModel], *, commit: bool = True) -> Any:
        deque(map(self.session.add, bulk))
        if commit:
            await self.session.commit()

    def query(
        self,
        query: SelectOfScalar | Select | None = None,
        filter_: BinaryExpression | None = None,
        *args,
        **kwargs,
    ) -> SelectOfScalar | Select:
        if query is None:
            query = select(cast("type[SQLModel]", self.__model__))
        elif not query.is_select:
            raise AssertionError("Only Select queries are available through query")
        if filter_:
            query = query.where(filter_)

        return query

    async def execute(self, query: SelectOfScalar | Select) -> Iterable[TSQLModel]:
        return await self.session.exec(query)
