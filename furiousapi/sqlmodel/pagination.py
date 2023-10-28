from __future__ import annotations

import datetime
import logging
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    cast,
)

import sqlalchemy as sa
from furiousapi.core.api.error_details import BadRequestHttpErrorDetails
from furiousapi.core.api.exceptions import FuriousAPIError
from furiousapi.core.config import get_settings
from furiousapi.core.db.pagination import (
    BaseCursorPagination,
    BaseRelayPagination,
    Cursor,
    OffsetPagination,
    PagePagination,
    PaginatorMixin,
)
from furiousapi.core.fields import SortingDirection
from furiousapi.core.pagination import PaginatedResponse, PaginationStrategyEnum
from pydantic import BaseConfig
from sqlalchemy import Integer, asc, desc, func

from sqlmodel.sql.expression import select

if TYPE_CHECKING:
    from furiousapi.core.db.fields import SortableFieldEnum
    from sqlalchemy.orm import InstrumentedAttribute
    from sqlalchemy.sql.elements import Cast, ColumnElement

    from sqlmodel import SQLModel
    from sqlmodel.ext.asyncio.session import AsyncSession
    from sqlmodel.sql.base import Executable
    from sqlmodel.sql.expression import Select

DEFAULT_PAGE_SIZE = get_settings().pagination.default_size
logger = logging.getLogger(__name__)

SORTING_FUNCS_MAPPING = {
    SortingDirection.DESCENDING: desc,
    SortingDirection.ASCENDING: asc,
}

_NULL = str(sa.null())


class SQLModelLimitMixin(PaginatorMixin):
    def __init__(self, session: AsyncSession) -> None:
        self.__session__ = session

    async def get_page(self, query: Select, limit: int, **kwargs) -> Tuple[List, bool]:
        query = query.limit(limit + 1)
        s = "\n" + str(query.compile(compile_kwargs={"literal_binds": True})) + "\n"
        logger.info(s)
        items = (await self.__session__.exec(query)).all()

        if limit is not None and len(items) > limit:
            has_next_page = True
            items = items[:limit]
        else:
            has_next_page = False

        return items, has_next_page


class SQLModelOffsetPaginatorMixin(SQLModelLimitMixin):
    def __init__(self, session: AsyncSession, model: Type[SQLModel]):
        super().__init__(session)
        self.model = model

    async def get_page(  # type: ignore[override]
        self, query: Select, limit: int, next_: int = 0, **kwargs
    ) -> PaginatedResponse:
        query = query.offset(next_)
        res = await super().get_page(query, limit, **kwargs)
        return PaginatedResponse(items=res[0], index=next_, next=next_ + limit)


class SQLModelOffsetPagination(SQLModelOffsetPaginatorMixin, OffsetPagination):
    pass


class SQLModelPagePaginationMixin(SQLModelOffsetPaginatorMixin, PagePagination):
    pass


class SQLModelCursorPagination(SQLModelLimitMixin, BaseCursorPagination):
    mapping: ClassVar[Dict[type, Callable]] = {
        sa.Boolean: int,
        bool: int,
        datetime.datetime: datetime.datetime.fromisoformat,
        sa.BIGINT: int,
        int: int,
    }

    def __init__(
        self,
        sort_enum: Type[SortableFieldEnum],
        id_fields: Set[str],
        sorting: List[SortableFieldEnum],
        session: AsyncSession,
        model: Type[SQLModel],
    ) -> None:
        self.model = model
        config: Type[BaseConfig] = cast(Type[BaseConfig], model.Config)
        self.__json_dumps__: Callable = config.json_dumps
        self.__json_loads__: Callable = config.json_loads
        super().__init__(session)
        super(SQLModelLimitMixin, self).__init__(sort_enum, id_fields, sorting)

    @staticmethod
    def _handle_nullable(column: InstrumentedAttribute, value: Any, *, is_nullable: bool) -> ColumnElement:
        if is_nullable:
            return sa.or_(column.is_(None), (column > value))

        return column > value

    def cast(self, column: InstrumentedAttribute, value: Any) -> Any:
        try:
            call = self.mapping[column.expression.type.python_type]
            if callable(call):
                return call(value)
        except TypeError:
            return None

    def get_filter(self, field_orderings: List[SortableFieldEnum], cursor: Cursor) -> ColumnElement:
        column_cursors = []
        for field, cursor_value in zip(field_orderings, cursor):
            column_cursors.append((getattr(self.model, field.name), field.direction, cursor_value))
        or_ = [self.get_filter_clause(column_cursors[: i + 1]) for i in range(len(column_cursors))]

        return sa.or_(*or_)

    def get_filter_clause(
        self, column_cursors: List[Tuple[InstrumentedAttribute, SortingDirection, Tuple[str, Any]]]
    ) -> Optional[ColumnElement]:
        previous_clauses = self.get_previous_clause(column_cursors[:-1])
        column, asc, value = column_cursors[-1]

        current_clause = self._prepare_current_clause(column, asc, value)

        if previous_clauses is None:
            return current_clause
        return sa.and_(previous_clauses, current_clause)

    def get_previous_clause(
        self, column_cursors: List[Tuple[InstrumentedAttribute, SortingDirection, Tuple[str, Any]]]
    ) -> Optional[ColumnElement]:
        if not column_cursors:
            return None
        clauses = []
        for column, _, value in column_cursors:
            value_ = self.cast(column, value[1])
            clauses.append(column.isnot_distinct_from(value_))

        return sa.and_(*clauses)

    def _prepare_current_clause(
        self, column: InstrumentedAttribute, asc: SortingDirection, value: Any
    ) -> Optional[ColumnElement]:
        is_nullable = getattr(column.expression, "nullable", True)
        value_ = value[1]
        if isinstance(value, bool):
            column: Cast[Integer, InstrumentedAttribute] = sa.cast(column, sa.Integer)
            value_ = int(value_)

        value_ = self.cast(column, value_)

        if asc == SortingDirection.ASCENDING:
            if value_ is None:
                return None
            if value_ is not None:
                current_clause = self._handle_nullable(column, value_, is_nullable=is_nullable)
            else:
                current_clause = column > value_
        else:
            current_clause = column.isnot(None) if value is None else column < value_

        return current_clause

    def encode_value(self, value: Any) -> str:
        if value is None:
            value = _NULL
        return super().encode_value(value)

    async def get_page_info(
        self,
        query: Select,
        field_orderings: List[SortableFieldEnum],
        cursor: Tuple[Tuple[str, Any], ...],
        items: List[SQLModel],
    ) -> dict:
        count_query = query.with_only_columns([func.count(*self.id_fields)]).order_by(None)
        logger.info("Count Query", extra={"query": str(count_query.compile(compile_kwargs={"literal_binds": True}))})
        total = (await self.__session__.execute(count_query)).scalar_one()
        index: Optional[int] = 0
        if cursor:
            inverted_ordering = [~field for field in field_orderings]
            filter_clause = self.get_filter(inverted_ordering, cursor)
            index_query = select(func.count()).select_from(query.filter(filter_clause).subquery())

            logger.debug(
                "Index Query", extra={"query": str(index_query.compile(compile_kwargs={"literal_binds": True}))}
            )
            index = (await self.__session__.execute(index_query)).scalar_one() + 1

            if self.reversed:
                before_index = total - index
                index = max(before_index - len(items), 0)

        if not items:
            index = None

        return {"index": index, "total": total}

    async def get_page(
        self, query: Union[Select, Executable], limit: int, next_: Optional[str] = None, **kwargs
    ) -> PaginatedResponse:
        field_orderings = self.get_field_orderings()

        cursor_in = self.parse_cursor(next_, field_orderings)

        page_query = query

        page_query = page_query.order_by(
            *[SORTING_FUNCS_MAPPING[sorting_field.direction](sorting_field.name) for sorting_field in field_orderings],
        )
        if cursor_in is not None:
            page_query = page_query.filter(self.get_filter(field_orderings, cursor_in))

        items, has_next_page = await super().get_page(page_query, limit, next_=next_)
        new_next = None
        if self.reversed:
            items.reverse()

        if items:
            cursors_out = self.render_cursor(items[-1], field_orderings)
            new_next = has_next_page and cursors_out or None

        page_info = await self.get_page_info(query, field_orderings, cursor_in, items)

        return PaginatedResponse[self.model](
            next=new_next,
            items=items,
            total=page_info["total"],
            index=page_info["index"],
        )


class SQLModelRelayCursorPagination(SQLModelCursorPagination, BaseRelayPagination):
    """A pagination scheme that works with the Relay specification.

    This pagination scheme assigns a cursor to each retrieved item. The page
    metadata will contain an array of cursors, one per item. The item metadata
    will include the cursor for the fetched item.

    For Relay Cursor Connections Specification, see
    https://facebook.github.io/relay/graphql/connections.htm.
    """

    async def get_page(
        self, query: Union[Select, Executable], limit: int, next_: Optional[str] = None, **kwargs
    ) -> PaginatedResponse:
        current_next = next_
        field_orderings = self.get_field_orderings()

        cursor_in = self.parse_cursor(current_next, field_orderings)

        page_query = query

        page_query = page_query.order_by(
            *[SORTING_FUNCS_MAPPING[sorting_field.direction](sorting_field.name) for sorting_field in field_orderings],
        )
        if cursor_in is not None:
            page_query = page_query.filter(self.get_filter(field_orderings, cursor_in))

        items, has_next_page = await super(SQLModelCursorPagination, self).get_page(
            page_query, limit, next_=current_next
        )
        new_next = None
        if self.reversed:
            items.reverse()

        if items:
            cursors_out = self.make_cursors(items, field_orderings)
            new_next = has_next_page and cursors_out[-1] or None

        page_info = await self.get_page_info(query, field_orderings, cursor_in, items)

        return PaginatedResponse[self.model](
            next=new_next,
            items=items,
            total=page_info["total"],
            index=page_info["index"],
        )


PAGINATION_MAPPING = {
    PaginationStrategyEnum.OFFSET: SQLModelOffsetPagination,
    PaginationStrategyEnum.CURSOR: SQLModelRelayCursorPagination,
}


def get_paginator(
    strategy: Union[PaginationStrategyEnum, str] = PaginationStrategyEnum.CURSOR,
) -> Union[Type[SQLModelOffsetPagination], Type[SQLModelCursorPagination]]:
    if not isinstance(strategy, Enum):
        strategy = PaginationStrategyEnum[strategy]
    if not (paginator := PAGINATION_MAPPING.get(strategy)):
        raise FuriousAPIError(BadRequestHttpErrorDetails(detail=f"pagination strategy {strategy} not found"))
    return paginator
