"""SQLite database manager for ShinBot runtime persistence."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from shinbot.persistence.config import DatabaseConfig
from shinbot.persistence.defaults import builtin_sliding_window_context_strategy
from shinbot.persistence.records import utc_now_iso
from shinbot.persistence.schema import apply_schema

from .repositories import (
    AgentRepository,
    AIInteractionRepository,
    AuditRepository,
    BotConfigRepository,
    ContextStrategyRepository,
    MediaAssetRepository,
    MediaSemanticRepository,
    MessageLogRepository,
    MessageMediaLinkRepository,
    ModelExecutionRepository,
    ModelRegistryRepository,
    PersonaRepository,
    PromptDefinitionRepository,
    PromptSnapshotRepository,
    SessionMediaOccurrenceRepository,
    SessionRepository,
)


class DatabaseManager:
    """Database bootstrap and repository access for the runtime data store."""

    def __init__(self, config: DatabaseConfig) -> None:
        self.config = config
        self.sessions = SessionRepository(self)
        self.audit = AuditRepository(self)
        self.agents = AgentRepository(self)
        self.bot_configs = BotConfigRepository(self)
        self.personas = PersonaRepository(self)
        self.prompt_definitions = PromptDefinitionRepository(self)
        self.context_strategies = ContextStrategyRepository(self)
        self.model_registry = ModelRegistryRepository(self)
        self.model_executions = ModelExecutionRepository(self)
        self.message_logs = MessageLogRepository(self)
        self.ai_interactions = AIInteractionRepository(self)
        self.prompt_snapshots = PromptSnapshotRepository(self)
        self.media_assets = MediaAssetRepository(self)
        self.message_media_links = MessageMediaLinkRepository(self)
        self.session_media_occurrences = SessionMediaOccurrenceRepository(self)
        self.media_semantics = MediaSemanticRepository(self)

        # Lazy-imported to avoid circular dependency (attention -> model_runtime -> persistence)
        from shinbot.agent.attention.repository import AttentionRepository, WorkflowRunRepository

        self.attention = AttentionRepository(self)
        self.workflow_runs = WorkflowRunRepository(self)

    @classmethod
    def from_bootstrap(
        cls,
        *,
        data_dir: Path | str,
        url: str | None = None,
        snapshot_ttl: int | None = None,
    ) -> DatabaseManager:
        return cls(
            DatabaseConfig.from_bootstrap(data_dir=data_dir, url=url, snapshot_ttl=snapshot_ttl)
        )

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
        self._ensure_builtin_context_strategies()

    def _ensure_builtin_context_strategies(self) -> None:
        now = utc_now_iso()
        self.context_strategies.upsert(builtin_sliding_window_context_strategy(now))
