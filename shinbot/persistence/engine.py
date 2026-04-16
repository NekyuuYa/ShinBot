"""SQLite database manager for ShinBot runtime persistence."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from shinbot.persistence.config import DatabaseConfig
from shinbot.persistence.repos import (
    AuditRepository,
    ContextStrategyRepository,
    ModelExecutionRepository,
    ModelRegistryRepository,
    PersonaRepository,
    SessionRepository,
)
from shinbot.persistence.schema import apply_schema


class DatabaseManager:
    """Database bootstrap and repository access for the runtime data store."""

    def __init__(self, config: DatabaseConfig) -> None:
        self.config = config
        self.sessions = SessionRepository(self)
        self.audit = AuditRepository(self)
        self.personas = PersonaRepository(self)
        self.context_strategies = ContextStrategyRepository(self)
        self.model_registry = ModelRegistryRepository(self)
        self.model_executions = ModelExecutionRepository(self)

    @classmethod
    def from_bootstrap(
        cls,
        *,
        data_dir: Path | str,
        url: str | None = None,
    ) -> DatabaseManager:
        return cls(DatabaseConfig.from_bootstrap(data_dir=data_dir, url=url))

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        """Open a transaction-scoped SQLite connection."""
        path = self.config.sqlite_path
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def initialize(self) -> None:
        """Create the database file and ensure the known schema exists."""
        with self.connect() as conn:
            apply_schema(conn)
