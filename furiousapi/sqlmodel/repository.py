from __future__ import annotations

from collections import deque
from functools import cached_property
from typing import TYPE_CHECKING, Any, List, Optional, Tuple, Type, TypeVar, Union, cast

from furiousapi.core.db.exceptions import EntityAlreadyExistsError, EntityNotFoundError
from furiousapi.core.db.repository import BaseRepository, RepositoryConfig
from furiousapi.core.pagination import (
    AllPaginationStrategies,
    PaginatedResponse,
    PaginationStrategyEnum,
)
from sqlalchemy import Column, and_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import load_only

from furiousapi.sqlmodel.models import SQLAllOptionalMeta, sql_model_query
from furiousapi.sqlmodel.pagination import get_paginator
from sqlmodel import SQLModel, select

if TYPE_CHECKING:
    from collections.abc import Iterable

    from furiousapi.core.types import TModelFields, TSortableFields

    from sqlmodel.ext.asyncio.session import AsyncSession

TSQLModel = TypeVar("TSQLModel", bound=SQLModel)


def _get_model_fields(model: Type[TSQLModel], fields: Iterable[TModelFields]) -> Iterable[str]:
    return (getattr(model, f.value) for f in fields)


class BaseSQLRepository(BaseRepository[TSQLModel]):
    __model__: Type[TSQLModel]

    class Config(RepositoryConfig):
        model_to_query = sql_model_query
        filter_model = SQLAllOptionalMeta

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    @cached_property
    def __primary_keys(self) -> Tuple[str, ...]:
        columns: List[Column] = self.__model__.__table__.primary_key.columns  # type: ignore[attr-defined]
        return tuple(column.name for column in columns)

    async def get(
        self,
        identifiers: Union[int, str, dict[str, Any], tuple[Any]],
        fields: Optional[Iterable[TModelFields]] = None,
        *,
        should_error: bool = True,
    ) -> Optional[TSQLModel]:
        options = []

        if fields:
            fields: Iterable[str] = _get_model_fields(self.__model__, fields)
            options.append(load_only(*fields))

        record = await self.session.get(self.__model__, identifiers, options=options)

        if not record and should_error:
            raise EntityNotFoundError(self.__model__, identifiers)

        return record

    async def list(
        self,
        pagination: AllPaginationStrategies,
        projection: Optional[Iterable[TModelFields]] = None,
        sorting: Optional[List[TSortableFields]] = None,
        filtering: Optional[TSQLModel] = None,
    ) -> PaginatedResponse[TSQLModel]:
        statement = select(cast(Type[SQLModel], self.__model__))
        if projection:
            projection: list[str] = list(_get_model_fields(self.__model__, projection))
            statement = statement.options(load_only(*projection))
        if not sorting and pagination.type == PaginationStrategyEnum.CURSOR:
            sorting = [+self.__sort__(self.__primary_keys[0])]

        if filtering and (to_filter := filtering.dict(exclude_unset=True, exclude_defaults=True)):
            where = [getattr(self.__model__, k) == v for k, v in to_filter.items()]
            statement = statement.where(and_(*where))

        init_params = {
            "session": self.session,
            "sorting": sorting,
            "id_fields": self.__primary_keys,
            "model": self.__model__,
            "sort_enum": self.__sort__,
        }

        paginator = get_paginator(pagination.type)(**init_params)  # type: ignore[arg-type]

        return await paginator.get_page(statement, pagination.limit, pagination.next)

    async def add(self, entity: TSQLModel, *, commit: bool = True) -> Optional[TSQLModel]:
        self.session.add(entity)

        if commit:
            try:
                await self.session.commit()
                await self.session.refresh(entity)
            except IntegrityError as exc:
                raise EntityAlreadyExistsError(self.__model__) from exc
            else:
                return entity

        return None

    async def update(self, entity: TSQLModel, *, commit: bool = True) -> Optional[TSQLModel]:
        self.session.add(entity)
        if commit:
            await self.session.commit()
            await self.session.refresh(entity)
            return entity
        return None

    async def delete(self, entity: Union[TSQLModel, str, int], **kwargs) -> None:
        return await self.session.delete(entity)

    async def bulk_create(self, bulk: List[TSQLModel], *, commit: bool = True) -> None:
        deque(map(self.session.add, bulk))
        if commit:
            return await self.session.commit()
        return None

    async def bulk_delete(self, bulk: List[Union[TSQLModel, Any]], *, commit: bool = True) -> None:
        deque(map(self.session.delete, bulk))
        if commit:
            await self.session.commit()

    async def bulk_update(self, bulk: List[TSQLModel], *, commit: bool = True) -> Any:
        deque(map(self.session.delete, bulk))
        if commit:
            await self.session.commit()
