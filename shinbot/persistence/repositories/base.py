"""Shared repository helpers and protocols."""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import Any


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _json_loads(value: str | None, default: Any) -> Any:
    if not value:
        return default
    return json.loads(value)


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
