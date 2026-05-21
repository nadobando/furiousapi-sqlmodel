from __future__ import annotations

from collections import deque
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    Any,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    Iterable,
    overload,
    Literal,
)

from fastapi._compat import PYDANTIC_V2
from furiousapi.db import BaseRepository, RepositoryConfig
from furiousapi.db import EntityAlreadyExistsError, EntityNotFoundError

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import load_only
from sqlmodel import SQLModel, select

from furiousapi.sqlmodel.models import SQLAllOptionalMeta, sql_model_query
from furiousapi.sqlmodel.pagination import get_paginator
from furiousapi.sqlmodel.utils import collect_relationships, dump_with_relationships

if TYPE_CHECKING:
    from sqlalchemy import Column
    from collections.abc import Iterable
    from sqlmodel.sql._expression_select_cls import SelectOfScalar, Select
    from furiousapi.core.types import TModelFields, TEntity
    from furiousapi.api.pagination import (
        AllPaginationStrategies,
        PaginatedResponse,
    )
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

    def __primary_values(self, instance: TSQLModel) -> tuple:
        return tuple(getattr(instance, i) for i in self.__primary_keys)

    @overload
    async def get(
        self,
        identifiers: Union[int, str, dict[str, Any], tuple[Any]],
        fields: Optional[Iterable[TModelFields]] = None,
        *,
        should_error: Literal[True] = True,
        options: Optional[List] = None,
    ) -> TSQLModel: ...

    @overload
    async def get(
        self,
        identifiers: Union[int, str, dict[str, Any], tuple[Any]],
        fields: Optional[Iterable[TModelFields]] = None,
        *,
        options: Optional[List] = None,
        should_error: Literal[False] = False,
    ) -> Optional[TSQLModel]: ...

    async def get(
        self,
        identifiers: Union[int, str, dict[str, Any], tuple[Any]],
        fields: Optional[Iterable[TModelFields]] = None,
        *,
        should_error: bool = True,
        options: Optional[List] = None,
    ) -> Optional[TSQLModel]:
        options = options or []

        if fields:
            fields: Iterable[str] = _get_model_fields(self.__model__, fields)
            options.append(load_only(*fields))

        record = await self.session.get(self.__model__, identifiers, options=options)

        if not record and should_error:
            raise EntityNotFoundError(self.__model__, identifiers)

        return record

    async def add(self, entity: TSQLModel, *, commit: bool = True) -> Optional[TSQLModel]:
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

    async def update(self, identifiers: Any, entity: TSQLModel, *, commit: bool = True) -> Optional[TSQLModel]:
        response = await self.get(identifiers)
        for key, value in entity.model_dump(exclude_unset=True, exclude=set(self.__primary_keys)).items():
            setattr(response, key, value)
        self.session.add(response)
        if commit:
            await self.session.commit()
            await self.session.refresh(response)
            return response
        return None

    async def delete(self, identifiers: tuple, *, commit: bool = True, **kwargs) -> None:
        response = await self.get(identifiers)
        delete_response = await self.session.delete(response)
        if commit:
            await self.session.commit()

        return delete_response

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
        deque(map(self.session.add, bulk))
        if commit:
            await self.session.commit()

    @overload
    async def query(
        self,
        query: SelectOfScalar | Select,
        pagination: "AllPaginationStrategies",
        *args,
        as_model: bool = True,
        return_cursor: Literal[False] = False,
        **kwargs,
    ) -> PaginatedResponse[TEntity]: ...

    @overload
    async def query(
        self,
        query: SelectOfScalar | Select,
        pagination: Literal[None] = None,
        *args,
        as_model: bool = True,
        return_cursor: Literal[False] = False,
        **kwargs,
    ) -> Iterable[TEntity]: ...

    async def query(
        self,
        query: SelectOfScalar | Select,
        pagination: Optional["AllPaginationStrategies"] = None,
        *args,
        as_model: bool = True,
        return_cursor: bool = False,
        **kwargs,
    ) -> Union[Iterable[TEntity], PaginatedResponse[TEntity]]:
        if query is None:
            query = select(cast("Type[SQLModel]", self.__model__))
        elif not query.is_select:
            raise AssertionError("Only Select queries are available through query")

        if pagination is not None:
            init_params = {
                "session": self.session,
                "id_fields": self.__primary_keys,
                "model": self.__model__,
            }

            paginator = get_paginator(pagination.pagination_type)(**init_params)  # type: ignore[arg-type]

            result, query, page_info = await paginator.get_page(query, pagination.limit, pagination.next)

            if result.items and not as_model:
                for i, item in enumerate(result.items):
                    result.items[i] = dump_with_relationships(item)

            return result

        result = await self.session.exec(query)
        if not return_cursor:
            result = result.all()
            if not as_model:
                result = [dump_with_relationships(i) for i in result]

        return result
