from example.models import Item, Review
from furiousapi.sqlmodel import SQLRepository


class ItemRepository(SQLRepository[Item]):
    pass


class ReviewRepository(SQLRepository[Review]):
    pass
