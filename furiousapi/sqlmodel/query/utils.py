from __future__ import annotations

from collections import defaultdict
from typing import List, Union, Tuple, Optional, Type
from typing import TYPE_CHECKING

from sqlalchemy.orm import InstrumentedAttribute, RelationshipProperty
from sqlalchemy.orm.strategy_options import _AbstractLoad, _WildcardLoad, load_only

if TYPE_CHECKING:
    from sqlmodel import SQLModel


def is_relationship(field: InstrumentedAttribute) -> bool:
    return isinstance(field.comparator, RelationshipProperty.Comparator)


def get_relation(field: InstrumentedAttribute) -> Type[SQLModel]:
    if not is_relationship(field):
        raise AssertionError("not a relation")
    return field.prop.mapper.class_


def build_field_options(load_fields: List[Union[InstrumentedAttribute, _WildcardLoad]]) -> List[_AbstractLoad]:
    field_groups = defaultdict(list)
    for field in load_fields:
        if isinstance(field, _WildcardLoad):
            return [field]
        field_groups[field.class_].append(field)
    return [load_only(*fields, raiseload=True) for fields in field_groups.values()]


def extract_field_info(
    nested: Tuple[str, Optional[Tuple[str, ...]]],
) -> tuple[str, bool, Optional[str], Optional[List[List[str]]]]:
    if isinstance(nested, list) and len(nested) > 1:
        return nested[0], True, nested[0], nested[1:]
    return nested[0], False, None, None
