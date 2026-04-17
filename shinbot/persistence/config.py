"""Database bootstrap configuration helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

DEFAULT_DB_DIRNAME = "db"
DEFAULT_DB_FILENAME = "shinbot.sqlite3"


def default_database_path(data_dir: Path | str) -> Path:
    """Return the default SQLite file path under the runtime data directory."""
    return Path(data_dir) / DEFAULT_DB_DIRNAME / DEFAULT_DB_FILENAME


def default_database_url(data_dir: Path | str) -> str:
    """Return the default SQLite URL for a given runtime data directory."""
    return f"sqlite:///{default_database_path(data_dir).resolve().as_posix()}"


def resolve_sqlite_path(url: str) -> Path:
    """Resolve a SQLite database URL into a filesystem path."""
    prefix = "sqlite:///"
    if not url.startswith(prefix):
        raise ValueError(f"Unsupported database URL: {url!r}")

    raw = url[len(prefix) :]
    if raw == ":memory:":
        raise ValueError("sqlite:///:memory: is not supported by the file-backed database manager")
    return Path(raw).expanduser().resolve()


@dataclass(slots=True)
class DatabaseConfig:
    """Normalized database bootstrap settings."""

    url: str
    sqlite_path: Path
    snapshot_ttl: int = 10800  # Default 3 hours

    @classmethod
    def from_bootstrap(
        cls,
        *,
        data_dir: Path | str,
        url: str | None = None,
        snapshot_ttl: int | None = None,
    ) -> DatabaseConfig:
        resolved_url = (
            url.strip() if isinstance(url, str) and url.strip() else default_database_url(data_dir)
        )
        return cls(
            url=resolved_url,
            sqlite_path=resolve_sqlite_path(resolved_url),
            snapshot_ttl=snapshot_ttl if snapshot_ttl is not None else 10800,
        )
