import time

from enum import IntEnum
from typing import List, Optional

from pydantic import BaseModel, Field


class Common(BaseModel):
    """For fields that we would like every recorded event to have."""

    timestamp_micros: int = Field(default_factory=lambda: int(time.time() * 1e6))


class User(BaseModel):
    """Minimalist user identifiers."""

    id: int


class SearchResult(BaseModel):
    """Minimalist search result info."""

    # A unique, persistent identifier for the returned document.
    document_id: int

    # The position of the document in the result list.
    position: int

    # The score of the document from the ranker.
    score: float


class SearchEvent(Common):
    """The information we want to keep/analyze about a search event."""

    # Information about the user who performed the query.
    user: User

    # A unique identifier for the query that we can use for joining the search event
    # to any subsequent click events.
    query_id: str

    # The raw query string the user typed in.
    raw_query: str

    # The results that were returned for the query.
    results: Optional[List[SearchResult]]


class ClickEvent(Common):
    """Information we want to record about a user click event."""

    # The query_id of the search event that generated this click.
    query_id: str

    # The id of the document that was clicked.
    document_id: int
