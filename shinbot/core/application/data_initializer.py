"""Runtime data directory initialization for boot."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path

REQUIRED_DATA_DIRS: tuple[str, ...] = (
    "",
    "db",
    "plugins",
    "plugin_data",
    "sessions",
    "audit",
    "agents",
    "personas",
    "prompts",
    "prompts/custom",
    "temp",
)


@dataclass(slots=True, frozen=True)
class DataInitializationResult:
    """Result of preparing the runtime data directory."""

    ensured_dirs: tuple[Path, ...]
    cleaned_temp_entries: tuple[Path, ...]


class DataInitializer:
    """Prepare filesystem state required before core services are created."""

    def __init__(self, data_dir: Path | str) -> None:
        self.data_dir = Path(data_dir)

    def initialize(self) -> DataInitializationResult:
        """Create required directories and clear ephemeral boot state."""

        self.data_dir.mkdir(parents=True, exist_ok=True)
        cleaned_temp_entries = self.cleanup_temp_directory()

        ensured_dirs: list[Path] = []
        for relative in REQUIRED_DATA_DIRS:
            path = self.data_dir / relative if relative else self.data_dir
            self.ensure_read_write(path)
            ensured_dirs.append(path)
        self.ensure_default_persona()
        self.ensure_model_registry_file()
        return DataInitializationResult(
            ensured_dirs=tuple(ensured_dirs),
            cleaned_temp_entries=tuple(cleaned_temp_entries),
        )

    def cleanup_temp_directory(self) -> tuple[Path, ...]:
        """Remove previous ephemeral temp entries while preserving ``data/temp``."""

        temp_dir = self.data_dir / "temp"
        if not temp_dir.exists():
            temp_dir.mkdir(parents=True, exist_ok=True)
            return ()

        removed: list[Path] = []
        for child in temp_dir.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
            removed.append(child)
        return tuple(removed)

    @staticmethod
    def ensure_read_write(directory: Path) -> None:
        """Create a directory and verify it is writable/readable."""

        directory.mkdir(parents=True, exist_ok=True)
        probe = directory / ".rw_probe"
        try:
            probe.write_text("ok", encoding="utf-8")
            _ = probe.read_text(encoding="utf-8")
        finally:
            if probe.exists():
                probe.unlink()

    def ensure_default_persona(self) -> Path:
        """Install the default editable persona markdown if it is missing."""

        from shinbot.admin.persona_files import PersonaFileRepository

        return PersonaFileRepository.from_data_dir(self.data_dir).ensure_default_persona()

    def ensure_model_registry_file(self) -> Path:
        """Create the editable model registry file if it is missing."""

        from shinbot.persistence.repositories.model_registry import ModelRegistryRepository

        return ModelRegistryRepository.from_data_dir(self.data_dir).ensure_file()


__all__ = [
    "DataInitializationResult",
    "DataInitializer",
    "REQUIRED_DATA_DIRS",
]
