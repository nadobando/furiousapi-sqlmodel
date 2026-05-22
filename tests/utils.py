from __future__ import annotations

import json
from typing import TYPE_CHECKING
from furiousapi.pydantic import PYDANTIC_V2

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest
    from pydantic import BaseModel


def get_first_doc_from_cache(request: FixtureRequest, cache_key: str, model: type[BaseModel] | None = None):
    docs = request.config.cache.get(cache_key, None)
    if not (docs and docs):
        return None
    first_doc_json_string = docs[0]
    if PYDANTIC_V2:
        return model.model_validate(first_doc_json_string) if model else json.loads(first_doc_json_string)
    return model.parse_raw(first_doc_json_string) if model else json.loads(first_doc_json_string)
