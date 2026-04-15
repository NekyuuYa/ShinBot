"""Prompt assembly logging utilities."""

from __future__ import annotations

import logging
from pathlib import Path

from shinbot.agent.prompting.schema import PromptLoggerRecord

logger = logging.getLogger(__name__)
prompt_logger = logging.getLogger("shinbot.prompt")


class PromptLogger:
    """Lightweight prompt assembly logger."""

    def __init__(self, data_dir: Path | str | None = None) -> None:
        self._data_dir: Path | None = None
        if data_dir is not None:
            self._data_dir = Path(data_dir)
            self._data_dir.mkdir(parents=True, exist_ok=True)

    def log(self, entry: PromptLoggerRecord) -> PromptLoggerRecord:
        prompt_logger.info(entry.to_json())
        if self._data_dir is not None:
            self._persist(entry)
        return entry

    def _persist(self, entry: PromptLoggerRecord) -> None:
        assert self._data_dir is not None
        target = self._data_dir / "prompt_assembly.jsonl"
        try:
            with target.open("a", encoding="utf-8") as file_obj:
                file_obj.write(entry.to_json() + "\n")
        except Exception:
            logger.exception("Failed to persist prompt log to %s", target)
