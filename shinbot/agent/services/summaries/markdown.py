"""Markdown mirror for persisted agent summaries."""

from __future__ import annotations

import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .models import SummaryType, SummaryWriteRequest

_UNSAFE_PATH_CHARS = re.compile(r"[^A-Za-z0-9._-]+")


class MarkdownSummaryStore:
    """Write summary records as human-readable Markdown files."""

    def __init__(self, root_dir: Path | str) -> None:
        self._root_dir = Path(root_dir)

    def save(
        self,
        record_id: int,
        request: SummaryWriteRequest,
        *,
        created_at: float,
    ) -> Path:
        """Persist one summary as a Markdown document and return the path."""

        target_dir = (
            self._root_dir
            / "sessions"
            / _safe_path_segment(request.session_id, fallback="unknown_session")
            / _safe_path_segment(request.summary_type.value, fallback="unknown_summary")
        )
        target_dir.mkdir(parents=True, exist_ok=True)
        path = _unique_timestamp_path(target_dir, created_at)
        content = _markdown_content(record_id, request, created_at=created_at)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(content, encoding="utf-8")
        os.replace(tmp, path)
        return path


def _unique_timestamp_path(directory: Path, created_at: float) -> Path:
    timestamp = created_at
    while True:
        path = directory / f"{_filename_timestamp(timestamp)}.md"
        if not path.exists():
            return path
        timestamp += 0.000001


def _filename_timestamp(created_at: float) -> str:
    value = datetime.fromtimestamp(created_at, UTC)
    return value.strftime("%Y-%m-%dT%H-%M-%S.") + f"{value.microsecond:06d}Z"


def _markdown_content(
    record_id: int,
    request: SummaryWriteRequest,
    *,
    created_at: float,
) -> str:
    metadata = _summary_metadata(request)
    frontmatter: dict[str, Any] = {
        "id": record_id,
        "session_id": request.session_id,
        "summary_type": request.summary_type.value,
        "source_run_id": request.source_run_id,
        "msg_count": request.msg_count,
        "created_at": created_at,
        "created_at_iso": datetime.fromtimestamp(created_at, UTC).isoformat(),
        "metadata_json": json.dumps(metadata, ensure_ascii=False),
    }
    if request.block_index is not None:
        frontmatter["block_index"] = request.block_index
    if request.msg_log_start is not None:
        frontmatter["msg_log_start"] = request.msg_log_start
    if request.msg_log_end is not None:
        frontmatter["msg_log_end"] = request.msg_log_end

    body = request.content.strip()
    metadata_block = ""
    if metadata:
        metadata_block = (
            "\n\n## Metadata\n\n"
            "```json\n"
            f"{json.dumps(metadata, ensure_ascii=False, indent=2)}\n"
            "```\n"
        )
    return (
        "+++\n"
        f"{_toml_lines(frontmatter)}"
        "+++\n\n"
        f"# {_summary_title(request.summary_type)}\n\n"
        f"{body}"
        f"{metadata_block}\n"
    )


def _summary_metadata(request: SummaryWriteRequest) -> dict[str, object]:
    metadata = dict(request.metadata)
    if request.block_index is not None:
        metadata.setdefault("block_index", request.block_index)
    if request.msg_count:
        metadata.setdefault("msg_count", request.msg_count)
    return metadata


def _toml_lines(values: dict[str, Any]) -> str:
    lines: list[str] = []
    for key, value in values.items():
        lines.append(f"{key} = {_toml_value(value)}\n")
    return "".join(lines)


def _toml_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float) and not isinstance(value, bool):
        return str(value)
    return json.dumps(str(value), ensure_ascii=False)


def _summary_title(summary_type: SummaryType) -> str:
    return summary_type.value.replace("_", " ").title()


def _safe_path_segment(value: str, *, fallback: str) -> str:
    normalized = _UNSAFE_PATH_CHARS.sub("_", str(value or "").strip()).strip("._-")
    return normalized or fallback


__all__ = ["MarkdownSummaryStore"]
