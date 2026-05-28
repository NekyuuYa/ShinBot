"""Path helpers for runtime configuration and project-local assets."""

from __future__ import annotations

from pathlib import Path

DEFAULT_DATA_DIR = Path("data")
DEFAULT_CONFIG_PATH = DEFAULT_DATA_DIR / "config.toml"


def project_root_from_config(config_path: Path | str | None) -> Path:
    """Infer the project root for paths that live outside the data directory."""
    if config_path is None:
        return Path.cwd().resolve()

    path = Path(config_path).resolve()
    parent = path.parent
    if parent.name == DEFAULT_DATA_DIR.name:
        return parent.parent.resolve()
    return parent.resolve()


def resolve_project_path(raw: str | Path, *, config_path: Path | str | None) -> Path:
    """Resolve a project-local path relative to the inferred project root."""
    path = Path(raw)
    if path.is_absolute():
        return path.resolve()
    return (project_root_from_config(config_path) / path).resolve()
