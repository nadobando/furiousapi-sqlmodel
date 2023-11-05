from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, Type

from furiousapi.db.metaclasses import model_query
from furiousapi.db.models import FuriousPydanticConfig
from furiousapi.db.utils import _remove_extra_data_from_signature
from pydantic import BaseModel

from sqlmodel import SQLModel
from sqlmodel.main import SQLModelMetaclass

if TYPE_CHECKING:
    from fastapi.params import Depends
    from pydantic.fields import ModelField


class SQLAllOptionalMeta(SQLModelMetaclass):
    def __new__(mcs, name: str, bases: Tuple[type], namespaces: Dict[str, Any], **kwargs) -> Any:  # noqa: N804
        __fields__ = {}
        for base in bases:
            if issubclass(base, FuriousSQLModel):
                for k, v in list(base.__fields__.items()):
                    v: ModelField
                    v.default = None
                    v.required = False
                    v.annotation = Optional[v.annotation]
                    # v.type
                    __fields__.update({k: v})

        mcs._convert_sqlmodel(name, namespaces, bases)
        new = super().__new__(mcs, name, (FuriousSQLModel,), namespaces, **kwargs)
        _remove_extra_data_from_signature(new)
        return new

    @staticmethod
    def _convert_sqlmodel(_: str, namespaces: dict, bases: tuple) -> None:
        annotations: dict = namespaces.get("__annotations__", {})

        for base in bases:
            for base_ in base.__mro__:
                if base_ is BaseModel or base_ is SQLModel:
                    break

                annotations.update(base_.__annotations__)

        for field in annotations:
            if not (field.startswith("__") or field == "metadata"):
                annotations[field] = Optional[annotations[field]]

        namespaces["__annotations__"] = annotations


class FuriousSQLModel(SQLModel):
    class Config(FuriousPydanticConfig): ...


def sql_model_query(model: Type[BaseModel]) -> Depends:
    return model_query(model, SQLModelMetaclass)
