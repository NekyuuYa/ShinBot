"""Prompt assembly logging utilities."""

from __future__ import annotations

import logging
from pathlib import Path

from shinbot.agent.prompt_manager.schema import PromptLoggerRecord
from shinbot.utils.logger import format_log_event

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
        prompt_logger.info(
            format_log_event(
                "prompt.assembly",
                profile=entry.profile_id,
                caller=entry.caller,
                session=entry.session_id,
                instance=entry.instance_id,
                route=entry.route_id,
                model=entry.model_id,
                components=entry.selected_component_count,
                unknown_sources=entry.unknown_source_count,
                compatibility=entry.compatibility_used,
                signature=entry.prompt_signature[:10] if entry.prompt_signature else None,
                metadata_keys=len(entry.metadata) if entry.metadata else None,
            )
        )
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
