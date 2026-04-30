"""Shared repository helpers and protocols."""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from sqlite3 import Connection
from typing import Any


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _json_loads(value: str | None, default: Any) -> Any:
    if not value:
        return default
    return json.loads(value)


class Repository:
    """Base class for repositories backed by DatabaseManager connections."""

    _MISSING = object()

    def __init__(self, db: Any, **dependencies: Any) -> None:
        self._db = db
        self._dependencies = dict(dependencies)

    @contextmanager
    def connect(self) -> Iterator[Connection]:
        with self._db.connect() as conn:
            yield conn

    def dependency(self, name: str, default: Any = _MISSING) -> Any:
        """Return an explicitly injected repository dependency."""
        if name in self._dependencies:
            return self._dependencies[name]
        if default is not self._MISSING:
            return default
        raise RuntimeError(f"Repository dependency {name!r} was not provided")

    def config_value(self, name: str, default: Any = _MISSING) -> Any:
        """Read a DatabaseConfig value through the repository boundary."""
        config = self._db.config
        if hasattr(config, name):
            return getattr(config, name)
        if default is not self._MISSING:
            return default
        raise RuntimeError(f"Database config value {name!r} was not provided")

    def json_dumps(self, value: Any) -> str:
        return _json_dumps(value)

    def json_loads(self, value: str | None, default: Any) -> Any:
        return _json_loads(value, default)

    def row_to_dict(
        self,
        row: Any,
        *,
        include: Iterable[str] | None = None,
        exclude: Iterable[str] = (),
        rename: Mapping[str, str] | None = None,
        bool_fields: Iterable[str] = (),
        json_fields: Mapping[str, tuple[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Map a sqlite row into a payload with bool and JSON field support."""
        rename = rename or {}
        json_fields = json_fields or {}
        json_sources = {source for source, _default in json_fields.values()}
        excluded = set(exclude) | json_sources
        keys = list(include) if include is not None else list(row.keys())

        payload = {
            rename.get(key, key): row[key]
            for key in keys
            if key not in excluded
        }
        for field in bool_fields:
            if field in payload:
                payload[field] = bool(payload[field])
        for target, (source, default) in json_fields.items():
            payload[target] = self.json_loads(row[source], default)
        return payload

    def rows_to_dicts(self, rows: Iterable[Any], **kwargs: Any) -> list[dict[str, Any]]:
        return [self.row_to_dict(row, **kwargs) for row in rows]


class ContextProvider(ABC):
    """Standardized session context retrieval interface."""

    @abstractmethod
    def get_recent(self, session_id: str, limit: int = 50) -> list[dict[str, Any]]:
        """Return recent session messages in chronological order."""

    @abstractmethod
    def get_by_time(
        self,
        session_id: str,
        start: float,
        end: float,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Return session messages within a time range in chronological order."""

    @abstractmethod
    def search_context(
        self,
        session_id: str,
        query: str,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Return matching session messages for keyword/semantic retrieval."""
