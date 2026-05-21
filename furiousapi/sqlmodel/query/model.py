from __future__ import annotations

from logging import getLogger
from typing import Type

from furiousapi.core.types import TEntity
from furiousapi.rql.models import ModelRQL

from furiousapi.sqlmodel.query.transform import SQLRQLTransform

LOGGER = getLogger(__name__)


class RQLModelSQL(ModelRQL[TEntity]):
    __model__: Type[TEntity]
    __transformer__ = SQLRQLTransform
