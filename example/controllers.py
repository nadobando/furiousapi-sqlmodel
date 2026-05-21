from __future__ import annotations
from typing import Optional, List, cast
from typing import TYPE_CHECKING

from fastapi import Path, Query
from furiousapi.api import ModelController
from sqlalchemy.orm import joinedload

from example.dependencies import item_repository, review_repository
from example.models import ItemRead, ItemCreate, ReviewCreate, Item
from furiousapi.sqlmodel.utils import dump_with_relationships

if TYPE_CHECKING:
    from furiousapi.core.types import TModelFields


class ItemController(ModelController, prefix="/item", tags=["Items"]):  # type: ignore[call-arg]
    repository = item_repository()
    get_model = ItemRead

    async def get(
        self, id_: int = Path(..., alias="id"), fields: Optional[List["TModelFields"]] = Query(None)
    ) -> ItemRead:
        res = await self.repository.get(id_, should_error=True, options=[joinedload(Item.reviews)], fields=fields)
        return ItemRead.model_validate(**dump_with_relationships(res))

    async def create(self, model: ItemCreate) -> ItemRead:
        return cast("ItemRead", await self.repository.add(cast("Item", model)))


class ReviewController(ModelController, prefix="/review", tags=["Reviews"]):  # type: ignore[call-arg]
    repository = review_repository()
    create_model = ReviewCreate
