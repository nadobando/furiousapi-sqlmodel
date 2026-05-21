from __future__ import annotations

import datetime
import json
import logging
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
    Iterable,
)

import pydantic_core
import sqlalchemy
import sqlalchemy as sa
from furiousapi.api.pagination import PaginatedResponse

from furiousapi.db.pagination import (
    BaseCursorPagination,
    BaseRelayPagination,
    Cursor,
    OffsetPagination,
    PagePagination,
    BasePagination,
)
from furiousapi.pydantic import PYDANTIC_V2
from sqlalchemy import Integer, Row, asc, desc, func
from sqlalchemy.sql.operators import desc_op, asc_op
from sqlmodel import SQLModel
from sqlmodel.sql.expression import select

from furiousapi.sqlmodel.utils import query_requires_unique

if TYPE_CHECKING:
    from sqlmodel.sql._expression_select_cls import SelectOfScalar
    from furiousapi.core.types import SortingDirection, Sorting
    from sqlalchemy.orm import InstrumentedAttribute
    from sqlalchemy.sql.elements import Cast, ColumnElement

    from sqlmodel.ext.asyncio.session import AsyncSession
    from sqlmodel.sql.base import Executable
    from sqlmodel.sql.expression import Select

    if PYDANTIC_V2:
        pass
    else:
        from pydantic import BaseConfig

SQLALCHEMY_V2 = int(sqlalchemy.__version__[0]) >= 2  # noqa: PLR2004

logger = logging.getLogger(__name__)

_NULL = str(sa.null())


class SQLModelLimitMixin(BasePagination):
    def __init__(self, session: AsyncSession) -> None:
        self.__session__ = session

    async def get_page(self, query: Select, limit: int, **kwargs) -> Tuple[List, bool]:
        query = query.limit(limit + 1)
        s = "\n" + str(query.compile(compile_kwargs={"literal_binds": True})) + "\n"
        logger.info(f"Page Query:{s}\n")
        result = await self.__session__.exec(query)
        if query_requires_unique(query):
            # session.exec returns a TupleResult whose .unique() lives on the
            # underlying Result; sqlmodel proxies it at runtime.
            items = result.unique().all()  # type: ignore[attr-defined]
        else:
            items = result.all()

        if limit is not None and len(items) > limit:
            has_next_page = True
            items = items[:limit]
        else:
            has_next_page = False

        return items, has_next_page


class SQLModelOffsetPaginatorMixin(SQLModelLimitMixin):
    def __init__(self, session: AsyncSession):
        super().__init__(session)

    async def get_page(self, query: Select, limit: int, next_: int = 0, **kwargs) -> PaginatedResponse:
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
        id_fields: Set[str],
        session: AsyncSession,
        model: Type[SQLModel],
    ) -> None:
        self.model = model
        if PYDANTIC_V2:
            self.__json_dumps__: Callable = pydantic_core.to_json
            self.__json_loads__: Callable = pydantic_core.from_json
        else:
            # v1-only path: SQLModel exposes pydantic v1 Config as a nested class;
            # v2 stubs don't declare it on the class.
            config: Type[BaseConfig] = cast("Type[BaseConfig]", model.Config)  # type: ignore[attr-defined]
            self.__json_dumps__: Callable = (hasattr(config, "json_dumps") and config.json_dumps) or json.dumps
            self.__json_loads__: Callable = (hasattr(config, "json_loads") and config.json_loads) or json.loads
        super().__init__(session)
        super(SQLModelLimitMixin, self).__init__(id_fields)

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

    def get_filter(self, field_orderings: List[Sorting], cursor: Cursor) -> ColumnElement:
        column_cursors = []
        for field, cursor_value in zip(field_orderings, cursor):
            if field.modifier == asc_op:
                sort = "asc"
            else:
                sort = "desc"

            if field.element.key is None:
                key = field.element.element.key
            else:
                key = field.element.key

            column_cursors.append((getattr(self.model, key), sort, cursor_value))
        or_clauses = [
            clause
            for i in range(len(column_cursors))
            if (clause := self.get_filter_clause(column_cursors[: i + 1])) is not None
        ]

        return sa.or_(*or_clauses)

    def get_filter_clause(
        self, column_cursors: List[Tuple[InstrumentedAttribute, SortingDirection, Tuple[str, Any]]]
    ) -> Optional[ColumnElement]:
        previous_clauses = self.get_previous_clause(column_cursors[:-1])
        column, asc, value = column_cursors[-1]

        current_clause = self._prepare_current_clause(column, asc, value)

        if previous_clauses is None:
            return current_clause
        if current_clause is None:
            return previous_clauses
        return sa.and_(previous_clauses, current_clause)

    def get_previous_clause(
        self, column_cursors: List[Tuple[InstrumentedAttribute, SortingDirection, Tuple[str, Any]]]
    ) -> Optional[ColumnElement]:
        if not column_cursors:
            return None
        clauses: List[ColumnElement] = []
        for column, _, value in column_cursors:
            value_ = self.cast(column, value[1])
            clauses.append(cast("ColumnElement", column.isnot_distinct_from(value_)))

        return sa.and_(*clauses)

    def _prepare_current_clause(
        self, column: InstrumentedAttribute, asc: SortingDirection, value: Any
    ) -> Optional[ColumnElement]:
        is_nullable = getattr(column.expression, "nullable", True)
        value_ = value[1]
        if isinstance(value, bool):
            column: Cast[Integer] = sa.cast(column, sa.Integer)
            value_ = int(value_)

        value_ = self.cast(column, value_)

        if asc == "asc":
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
        field_orderings: List[Sorting],
        cursor: Tuple[Tuple[str, Any], ...],
        items: List[SQLModel],
    ) -> dict:
        count_query: Any
        if SQLALCHEMY_V2:
            count_query = select(func.count()).select_from(query.subquery())
        else:
            count_query = query.with_only_columns(func.count(*self.id_fields))

        count_query.order_by(None)
        query_string = str(count_query.compile(compile_kwargs={"literal_binds": True}))

        logger.info(f"Count Query\n {query_string}\n", extra={"query": query_string})
        total = (await self.__session__.exec(count_query)).first() or 0
        index: Optional[int] = 0
        if cursor:
            inverted_ordering = []
            order_by_clauses = list(query._order_by_clauses)  # noqa: SLF001
            all_order_by_clauses = order_by_clauses + field_orderings
            for i in all_order_by_clauses:
                if i.modifier == asc_op:
                    inverted_ordering.append(desc(i.element))
                else:
                    inverted_ordering.append(asc(i.element))
            filter_clause = self.get_filter(inverted_ordering, cursor)
            index_query = select(func.count()).select_from(query.filter(filter_clause).subquery())

            logger.info(f"Index Query:\n {index_query.compile(compile_kwargs={'literal_binds': True})!s}\n")
            index_count = (await self.__session__.exec(index_query)).first() or 0
            index = index_count + 1

            if self.reversed:
                before_index = total - index
                index = max(before_index - len(items), 0)

        if not items:
            index = None

        return {"index": index, "total": total}

    async def get_page(
        self, query: Union[Select, Executable], limit: int, next_: Optional[str] = None, **kwargs
    ) -> PaginatedResponse:
        # The Union accepts Executable for caller flexibility, but the pagination
        # logic depends on Select-only methods (_order_by_clauses, order_by,
        # filter). Narrow here.
        select_query = cast("Select", query)
        field_orderings = self.get_field_orderings(select_query)

        order_by_clauses = list(select_query._order_by_clauses)  # noqa: SLF001
        all_order_by_clauses = order_by_clauses + field_orderings
        cursor_in = self.parse_cursor(next_, all_order_by_clauses)

        page_query = select_query.order_by(*field_orderings)
        if cursor_in is not None:
            page_query = page_query.filter(self.get_filter(all_order_by_clauses, cursor_in))

        items, has_next_page = await super().get_page(page_query, limit, next_=next_)

        new_next = None
        if self.reversed:
            items.reverse()

        if items:
            if items[-1] is Row:
                item = items[-1][0]
            else:
                item = items[-1]
            cursors_out = self.render_cursor(cast("SQLModel", item), all_order_by_clauses)
            new_next = (has_next_page and cursors_out) or None

        page_info = await self.get_page_info(select_query, field_orderings, cursor_in, items)
        if PYDANTIC_V2:
            result = (
                PaginatedResponse[self.model].model_construct(
                    next=new_next,
                    items=items,
                    total=page_info["total"],
                    index=page_info["index"],
                )
                # page_query,
                # page_info,
            )
        else:
            result = (
                PaginatedResponse[self.model](
                    next=new_next,
                    items=items,
                    total=page_info["total"],
                    index=page_info["index"],
                )
                # page_query,
                # page_info,
            )
        return result

    def transform_to_model_fields(self, row_dict: dict) -> dict:
        mapper = sqlalchemy.inspect(self.model)
        if mapper is None:
            return row_dict
        for rel in mapper.relationships:
            rel_name = rel.key
            rel_class_name = rel.entity.class_.__name__
            if rel_class_name in row_dict:
                row_dict[rel_name] = row_dict.pop(rel_class_name)
        return row_dict

    def _get_nested_value(self, item: Any, field: ColumnElement) -> Any:
        try:
            # Unwrap UnaryExpression (like -column)
            while hasattr(field, "element"):
                field = field.element

            # Now extract the full attribute path (if any)
            attr_path: List[Any] = []  # TODO: check this hint
            current = field
            while hasattr(current, "left") and hasattr(current.left, "key"):
                if current.key is not None:
                    attr_path.insert(0, current.key)
                current = current.left

            # Fallback if it's just a column
            if not attr_path and hasattr(current, "key"):
                attr_path.append(current.key)

            value = item
            for attr in attr_path:
                value = getattr(value, attr, None)
                if value is None:
                    return None

        except Exception as e:
            raise AttributeError(f"Failed to resolve value for {field!r}") from e
        else:
            return value

    def render_cursor(self, item: SQLModel, column_fields: Iterable[ColumnElement[Any]]) -> str:
        if PYDANTIC_V2:
            cursor_ = []
            for field in column_fields:
                if isinstance(item, SQLModel):
                    dumped_value = self.__json_dumps__(self._get_nested_value(item, field))

                else:
                    dumped_value = self.__json_dumps__(item)
                cursor_.append(dumped_value.decode())
            cursor = tuple(cursor_)
        else:
            # In v1 mode, column_fields entries are UnaryExpression at runtime;
            # `.element.key` is the underlying column name.
            cursor = tuple(
                self.__json_dumps__(getattr(item, field.element.key or ""), default=str) for field in column_fields
            )
        return self.encode_cursor(cursor)

    def get_field_orderings(self, query: Union[Select, SelectOfScalar]) -> list[ColumnElement[Any]]:
        sorting = list(query._order_by_clauses) or []  # noqa: SLF001
        sorting_fields = set()
        for sort in sorting:
            namespace = sort.element.entity_namespace
            sorting_fields.add((sort.element.key, namespace if isinstance(namespace, SQLModel) else sort.element.table))
        if sorting:
            op = asc_op if sorting[-1].modifier == asc_op else desc_op
        else:
            op = asc_op

        return cast(
            "list[ColumnElement[Any]]",
            [
                op(getattr(self.model, id_field))
                for id_field in self.id_fields
                if (id_field, self.model) not in frozenset(sorting_fields)
            ],
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
        select_query = cast("Select", query)

        field_orderings = self.get_field_orderings(select_query)

        cursor_in = self.parse_cursor(current_next, field_orderings)

        page_query = select_query.order_by(*field_orderings)
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
            new_next = (has_next_page and cursors_out[-1]) or None

        page_info = await self.get_page_info(select_query, field_orderings, cursor_in, items)
        return PaginatedResponse[self.model](
            next=new_next,
            items=items,
            total=page_info["total"],
            index=page_info["index"],
        )
