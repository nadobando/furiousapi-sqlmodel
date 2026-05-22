from typing import Optional

from sqlmodel import SQLModel, Field, Relationship


class BaseReview(SQLModel):
    text: str


class Review(BaseReview, table=True):  # type: ignore[call-arg]
    id: int | None = Field(default=None, primary_key=True)
    item_id: int = Field(foreign_key="item.id")
    item: Optional["Item"] = Relationship(back_populates="reviews", sa_relationship_kwargs={"uselist": False})


class ReviewItemRead(BaseReview):
    id: int


class ReviewRead(ReviewItemRead):
    item_id: int


class ReviewCreate(BaseReview):
    item_id: int


class BaseItem(SQLModel):
    name: str
    description: str = Field(default=None, nullable=True)


class Item(BaseItem, table=True):  # type: ignore[call-arg]
    id: int | None = Field(default=None, primary_key=True)
    reviews: list["Review"] | None = Relationship(back_populates="item", sa_relationship_kwargs={"uselist": True})


class ItemCreate(BaseItem):
    pass


class ItemRead(BaseItem):
    id: int
    reviews: list[ReviewItemRead] | None
