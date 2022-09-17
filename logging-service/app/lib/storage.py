import abc
import collections
import itertools
from typing import List, Optional

from . import contracts


def get_store():
    """Returns the storage engine to use for persisting logged records."""
    return InMemoryStore()


class Store(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def write(self, log_record: contracts.Envelope):
        ...

    @abc.abstractmethod
    def fetch(
        self, log_type: Optional[contracts.LogType], start: int, limit: int
    ) -> List[contracts.Envelope]:
        ...


class InMemoryStore(Store):
    """An in-memory only store, useful for testing and demo purposes."""

    def __init__(self, maxlen: int = 1000):
        self._logs = collections.deque(maxlen=maxlen)

    def write(self, log_record: contracts.Envelope):
        self._logs.appendleft(log_record)

    def fetch(
        self, log_type: Optional[contracts.LogType], start: int, limit: int
    ) -> List[contracts.Envelope]:
        if log_type is None:
            return list(itertools.islice(self._logs, start, limit))
        else:
            matches = [log for log in self._logs if log.log_type == log_type]
            return list(itertools.islice(matches, start, limit))
