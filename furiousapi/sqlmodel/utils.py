from __future__ import annotations

from contextlib import suppress
from typing import Any
from typing import TYPE_CHECKING

import sqlalchemy
from sqlalchemy.inspection import inspect as sa_inspect
from sqlalchemy.orm.strategy_options import _AttributeStrategyLoad

if TYPE_CHECKING:
    from sqlalchemy.orm import InstrumentedAttribute
    from sqlmodel import SQLModel
    from sqlalchemy.sql import Select
    from pydantic import BaseModel
    from sqlalchemy.sql.base import ReadOnlyColumnCollection


def collect_relationships(
    model: type[SQLModel], entity: SQLModel | BaseModel, visited: set | None = None
) -> dict[str, SQLModel]:
    rels = {}
    if visited is None:
        visited = set()
    for relationship in model.__sqlmodel_relationships__:
        rel = getattr(entity, relationship, None)

        if not rel:
            continue
        class_: SQLModel = getattr(model, relationship).mapper.class_
        rels[relationship] = class_.model_validate(rel)
        relation_class: type[SQLModel] = type(rel)
        if rel.__sqlmodel_relationships__ and relation_class.__name__ not in visited:
            visited.add(relation_class.__name__)
            rels.update(collect_relationships(relation_class, rel, visited))

    return rels


def dump_with_relationships(
    obj: SQLModel,
    *,
    path: list[int] | None = None,
    parent_model: type[SQLModel] | None = None,
    parent_rel_name: str | None = None,
    indent: int = 0,
) -> dict[str, Any]:
    path = path or []
    obj_id = id(obj)

    if obj_id in path:
        return obj.model_dump()

    path.append(obj_id)
    mapper = sa_inspect(obj.__class__)
    data = obj.model_dump()
    if mapper is None:
        return data

    for rel in mapper.relationships:
        rel_name = rel.key

        # 🛑 Skip if it's a back-reference to the parent
        if parent_model and rel.back_populates == parent_rel_name:
            continue

        rel_value = None
        with suppress(sqlalchemy.exc.MissingGreenlet, sqlalchemy.exc.InvalidRequestError):
            rel_value = getattr(obj, rel_name, None)

        if rel_value is None:
            continue

        if rel.uselist:
            data[rel_name] = [
                dump_with_relationships(
                    item, path=list(path), parent_model=obj.__class__, parent_rel_name=rel_name, indent=indent + 2
                )
                for item in rel_value
            ]
        else:
            nested_data = dump_with_relationships(
                rel_value, path=list(path), parent_model=obj.__class__, parent_rel_name=rel_name, indent=indent + 1
            )
            data[rel_name] = nested_data

        # 🔥 After processing rel_value: if it's not None, remove FKs pointing to it
        if not rel.uselist and rel_value is not None:
            for fk in rel.local_columns:
                data.pop(fk.name, None)

    return data


def query_requires_unique(query: Select) -> bool:
    if not hasattr(query, "_with_options"):
        return False

    for opt in query._with_options:  # noqa: SLF001
        has_eager = False

        for ctx in getattr(opt, "context", []):
            if isinstance(ctx, _AttributeStrategyLoad) and "eager_from_alias" in dict(ctx.local_opts):
                has_eager = True
                break

        if not has_eager:
            continue

        path = getattr(opt, "path", None)
        if not path:
            continue

        for i in path:
            if getattr(i, "uselist", None):
                return True

    return False


def model_primary_keys_fields(model: type[SQLModel]) -> tuple[InstrumentedAttribute, ...]:
    # __table__ is added by SQLAlchemy on mapped (table=True) SQLModels at class
    # creation time; not declared on the SQLModel stub itself.
    columns: list[ReadOnlyColumnCollection] = model.__table__.primary_key.columns  # type: ignore[attr-defined]
    return tuple(getattr(model, column.name) for column in columns)
