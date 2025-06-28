from __future__ import annotations

import operator
from logging import getLogger
from typing import TYPE_CHECKING, cast
from typing import Type, List, Union, Dict, Any, Tuple, Optional

from furiousapi.rql.transform import BaseRQLModelTransform
from sqlalchemy.orm import InstrumentedAttribute, contains_eager, aliased, Load
from sqlalchemy.orm import load_only

# noinspection PyProtectedMember
from sqlalchemy.orm.strategy_options import undefer
from sqlalchemy.sql.operators import asc_op
from sqlmodel import SQLModel, and_, func, or_, select

from furiousapi.sqlmodel.query import utils
from furiousapi.sqlmodel.utils import model_primary_keys_fields

if TYPE_CHECKING:
    from sqlalchemy.orm.util import AliasedClass
    from lark import Tree, Token
    from sqlalchemy import Select, ColumnElement

    # noinspection PyProtectedMember
    from sqlalchemy.sql.functions import _FunctionGenerator, Function
    from sqlalchemy.sql.elements import BinaryExpression

    # noinspection PyProtectedMember
    from sqlalchemy.orm.strategy_options import _AbstractLoad

    ProcessNestedFieldResult = (
        tuple[AliasedClass | Type[SQLModel], _AbstractLoad, list[Any]]
        | tuple[AliasedClass | Type[SQLModel], None, list[_AbstractLoad]]
    )

LOGGER = getLogger(__name__)


class SQLRQLTransform(BaseRQLModelTransform):
    model: Type[SQLModel]
    _options: List
    _where: List

    _joins: List
    _aliases: Dict[str, Union[AliasedClass, Type[SQLModel]]]
    _contains_eager_options: List

    def __init__(self, model: Type[SQLModel], *args, **kwargs) -> None:
        super().__init__(model, *args, **kwargs)
        self._options = []
        self._where = []
        self._joins = []
        self._aliases = {}
        self._contains_eager_options = []
        self._aliases = {"": model}

    def start(self, _: Tree) -> Select:
        self._process_select(self.__selected_fields__)
        q = select(self.model)
        updated_sorting = self._build_sort_clauses()

        if self.include_cursor_sort_fields:
            self._ensure_sort_fields_loaded()
        q = q.options(*self._contains_eager_options, *self._options)

        for _, rel_attr, alias in self._joins:
            q = q.join(alias, rel_attr, isouter=True)

        q = q.where(*self.__filter_fields__)
        q = q.order_by(*updated_sorting)

        LOGGER.debug(str(q.compile()))
        return q

    @staticmethod
    def search_term(s: Tuple[Token]) -> _FunctionGenerator:
        return getattr(func, s[0])

    def searching(self, s: Tuple[_FunctionGenerator, str, str]) -> Function:
        if not isinstance(s[2], str):
            raise TypeError("value must be string")
        field = self._get_field(s[1])
        field_path = s[1]

        path, _, field_name = field_path.rpartition(".")
        alias = self._get_or_create_alias(path, self.model)
        attr = getattr(alias, field_name)
        if str(field.type) != "VARCHAR":
            raise AttributeError("attribute must be string attribute")
        return s[0](attr, s[2])

    def comp(self, term: Tuple[str, str, Any]) -> BinaryExpression:
        op = super().comp(term)[0]
        field_path = term[1]
        value = term[2]

        path, _, field_name = field_path.rpartition(".")
        alias = self._get_or_create_alias(path, self.model)
        attr = getattr(alias, field_name)

        return getattr(attr, op)(value)

    def and_(self, terms: List[BinaryExpression]) -> ColumnElement[bool]:
        return and_(*terms)

    def or_(self, terms: List[BinaryExpression]) -> ColumnElement[bool]:
        return or_(*terms)

    def listing(self, term: Tuple[str, str, Any]) -> BinaryExpression:
        field_path = term[1]
        path, _, field_name = field_path.rpartition(".")
        alias = self._get_or_create_alias(path, self.model)
        field = getattr(alias, field_name)
        return field.in_(term[2:])

    @staticmethod
    def not_(token: List[BinaryExpression]) -> BinaryExpression:
        return ~token[0]

    def _build_sort_clauses(self) -> List[ColumnElement]:
        clauses = []
        for path, direction in self.__sorting_fields__:
            sort_path, _, field = path.rpartition(".")
            alias = self._get_or_create_alias(sort_path, self.model)
            attr = getattr(alias, field)
            clauses.append(asc_op(attr) if direction == operator.pos else -attr)
        return clauses

    def _ensure_sort_fields_loaded(self) -> None:
        for sort_field, _ in self.__sorting_fields__:
            parts = sort_field.split(".")
            if len(parts) == 1:
                field = getattr(self.model, sort_field, None)
                if field:
                    self._options.append(load_only(field, raiseload=True))
            else:
                *rel_path, attr = parts
                path = ".".join(rel_path)
                alias = self._get_alias(path)

                if not alias:
                    continue
                try:
                    field = getattr(alias, attr)
                except AttributeError:
                    continue
                eager = self._build_eager_chain(path, field)
                self._contains_eager_options.append(eager)

    def _get_field(self, field: str, current_model: Optional[Type[SQLModel]] = None) -> InstrumentedAttribute:
        attributes = field.split(".")
        model = current_model or self.model
        field_: Optional[InstrumentedAttribute] = None
        for attr in attributes:
            field_: InstrumentedAttribute = cast("InstrumentedAttribute", operator.attrgetter(attr)(model))
            if utils.is_relationship(field_) and field_ is not None:
                model = field_.prop.mapper.class_

        if field_ is None:
            raise AssertionError
        return field_

    def _process_select(self, sel: Dict) -> None:
        selected_root_fields = set()
        selected_relations = set()
        for field_name, value in sel.items():
            if value:
                selected_relations.add(field_name)
                eager_option, eager_fields = self._process_nested_fields(field_name, value, self.model)
                if eager_option:
                    self._contains_eager_options.append(eager_option)
            elif field_name == "*":
                selected_root_fields.add(field_name)
            else:
                field = self._get_field(field_name)
                if utils.is_relationship(field):
                    eager_option, fields = self._resolve_field_loader(field_name, self.model, "")
                    self._contains_eager_options.append(eager_option)
                    selected_relations.add(field_name)
                else:
                    selected_root_fields.add(field_name)
                    selected_field = getattr(self.model, field_name)
                    self._options.append(load_only(selected_field, raiseload=True))

        if not selected_root_fields and selected_relations:
            self._options.append(load_only(*model_primary_keys_fields(self.model)))

    def _process_nested_fields(
        self, base_field: str, selection: Dict, current_model: Type[SQLModel], prefix: str = ""
    ) -> Tuple[_AbstractLoad, List[InstrumentedAttribute]]:
        selected_fields = set()
        selected_relations = set()

        path = f"{prefix}.{base_field}" if prefix else base_field
        alias = self._get_or_create_alias(path, current_model)

        eager_opts: list[_AbstractLoad] = []
        load_fields: list[InstrumentedAttribute] = []

        for field_name, nested_selection in selection.items():
            if nested_selection:
                selected_relations.add(field_name)
                current_attr = getattr(current_model, base_field)
                nested_model = utils.get_relation(current_attr)
                child_eager, child_fields = self._process_nested_fields(
                    field_name, nested_selection, nested_model, path
                )
                eager_opts.extend(
                    child_eager if isinstance(child_eager, list) else [child_eager] if child_eager else []
                )
            else:
                child_eager, child_fields = self._resolve_field_loader(field_name, alias, path)
                if child_eager:
                    selected_relations.add(field_name)
                    eager_opts.append(child_eager)
                else:
                    selected_fields.add(field_name)
                load_fields.extend(child_fields)

        local_opts = utils.build_field_options(load_fields)
        if selected_relations and not local_opts:
            local_opts = [load_only(*[getattr(alias, x.name) for x in model_primary_keys_fields(current_model)])]

        source = self._get_alias(prefix) if prefix else self.model
        relationship_attr = getattr(source, base_field)

        if not utils.is_relationship(relationship_attr):
            raise ValueError(f"Cannot eager-load non-relationship attribute: {relationship_attr}")

        inner = Load(alias).options(*eager_opts, *local_opts)
        return contains_eager(relationship_attr.of_type(alias)).options(inner), load_fields

    def _resolve_field_loader(
        self, field_name: str, alias: Union[AliasedClass, Type[SQLModel]], path: str
    ) -> tuple[Optional[_AbstractLoad], list[Any]]:
        if field_name == "*":
            return None, [undefer("*")]
        field = getattr(alias, field_name)
        if utils.is_relationship(field):
            rel_model = getattr(alias, field_name).property.mapper.class_
            rel_path = f"{path}.{field_name}" if path else field_name
            rel_alias = self._get_or_create_alias(rel_path, rel_model)
            eager = contains_eager(getattr(alias, field_name).of_type(rel_alias))
            return eager, []
        return None, [field]

    def _build_eager_chain(self, path: str, field: InstrumentedAttribute) -> Load:
        parts = path.split(".")
        current_path = []
        cur = Load(self.model)
        for i, segment in enumerate(parts):
            current_path.append(segment)
            sub_path = ".".join(current_path)
            alias = self._get_alias(sub_path)

            if alias is None:
                raise RuntimeError(f"Missing alias for path: {sub_path}")

            rel_path = ".".join(current_path[:-1]) or ""
            rel_alias = self._get_alias(rel_path) or self.model
            rel_attr = getattr(rel_alias, segment)
            if i == len(parts) - 1:
                # noinspection PyTestUnpassedFixture
                cur = cur.contains_eager(rel_attr.of_type(alias)).options(load_only(field, raiseload=True))
            else:
                cur = cur.contains_eager(rel_attr.of_type(alias))
        return cur

    # region: alias & join resolution
    def _get_alias(self, path: str) -> Optional[AliasedClass]:
        return cast("Optional[AliasedClass]", self._aliases.get(path))

    def _get_or_create_alias(self, path: str, model: Type[SQLModel]) -> AliasedClass:
        return self._get_alias(path) or self._create_alias(path, model)

    # endregion

    def _create_alias(self, path: str, model: Type[SQLModel]) -> AliasedClass:
        parts = path.split(".")
        parent_path = ".".join(parts[:-1])
        rel_name = parts[-1]

        parent_alias = self._get_alias(parent_path) or self._create_alias(parent_path, model)
        rel_attr = getattr(parent_alias, rel_name)
        rel_model = rel_attr.property.mapper.class_

        alias = aliased(rel_model)
        self._aliases[path] = alias
        self._joins.append((parent_alias, rel_attr, alias))

        return alias
