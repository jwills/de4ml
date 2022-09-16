import time

from enum import IntEnum
from typing import List, Optional

from pydantic import BaseModel


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


class SearchLog(BaseModel):
    """The information we want to keep/analyze about a search event."""

    # Information about the user who performed the query.
    user: User

    # A unique identifier for the query that we can use for joining the search event
    # to any subsequent click events.
    query_id: str

    # The raw query string the user typed in.
    raw_query: str

    # The results that were returned for the query.
    results: List[SearchResult]


class ClickLog(BaseModel):
    """Information we want to record about a user click event."""

    # The query_id of the search event that generated this click.
    query_id: str

    # The id of the document that was clicked.
    document_id: int


class LogType(IntEnum):
    """An enum to represent the different kind of logged events for the envelope."""

    SEARCH = 1
    CLICK = 2


class Envelope(BaseModel):
    """The envelope provides certain universal fields for all logged events."""

    # The timestamp of when this log event was created.
    timestamp_micros: int = int(time.time() * 1e6)

    # The type of this logged record, tells us what data to expect to be populated
    log_type: LogType

    # The data for SEARCH logs
    search: Optional[SearchLog] = None

    # The data for CLICK logs
    click: Optional[ClickLog] = None
