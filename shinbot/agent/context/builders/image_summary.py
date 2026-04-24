"""Persistent image summary registry for context-stage rendering."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from shinbot.agent.context.state.state_store import ContextSessionState


@dataclass(slots=True)
class ImageSummaryEntry:
    raw_hash: str = ""
    strict_dhash: str = ""
    summary_text: str = ""
    kind: str = ""
    is_custom_emoji: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "raw_hash": self.raw_hash,
            "strict_dhash": self.strict_dhash,
            "summary_text": self.summary_text,
            "kind": self.kind,
            "is_custom_emoji": self.is_custom_emoji,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> ImageSummaryEntry:
        return cls(
            raw_hash=str(payload.get("raw_hash", "") or ""),
            strict_dhash=str(payload.get("strict_dhash", "") or ""),
            summary_text=str(payload.get("summary_text", "") or ""),
            kind=str(payload.get("kind", "") or ""),
            is_custom_emoji=bool(payload.get("is_custom_emoji", False)),
            metadata=dict(payload.get("metadata", {})),
        )


@dataclass(slots=True)
class ResolvedImageReference:
    image_id: str
    raw_hash: str = ""
    strict_dhash: str = ""
    summary_text: str = ""
    kind: str = ""
    is_custom_emoji: bool = False


class ContextImageRegistry:
    """Persist image digests by dual hash and assign short session-local IDs."""

    def __init__(self, data_dir: Path | str | None = "data") -> None:
        self._path: Path | None = None
        self._entries: dict[str, ImageSummaryEntry] = {}
        if data_dir is not None:
            self._path = Path(data_dir) / "temp" / "context_images.json"
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._load()

    def get_or_create_reference(
        self,
        *,
        session_state: ContextSessionState,
        raw_hash: str,
        strict_dhash: str,
        summary_text: str = "",
        kind: str = "",
        is_custom_emoji: bool = False,
        metadata: dict[str, Any] | None = None,
    ) -> ResolvedImageReference:
        key = self.make_key(raw_hash=raw_hash, strict_dhash=strict_dhash)
        if not key:
            key = self.make_key(raw_hash=summary_text.strip(), strict_dhash=kind.strip())

        entry = self._entries.get(key)
        changed = False
        if entry is None:
            entry = ImageSummaryEntry(
                raw_hash=raw_hash,
                strict_dhash=strict_dhash,
                summary_text=summary_text.strip(),
                kind=kind.strip(),
                is_custom_emoji=is_custom_emoji,
                metadata=dict(metadata or {}),
            )
            self._entries[key] = entry
            changed = True
        else:
            next_summary = summary_text.strip()
            next_kind = kind.strip()
            if next_summary and next_summary != entry.summary_text:
                entry.summary_text = next_summary
                changed = True
            if next_kind and next_kind != entry.kind:
                entry.kind = next_kind
                changed = True
            if is_custom_emoji and not entry.is_custom_emoji:
                entry.is_custom_emoji = True
                changed = True
            if metadata:
                merged = dict(entry.metadata)
                merged.update(metadata)
                if merged != entry.metadata:
                    entry.metadata = merged
                    changed = True

        numeric_id = session_state.image_ids.assign(key)
        if changed:
            self._save()

        return ResolvedImageReference(
            image_id=f"{numeric_id:04d}",
            raw_hash=entry.raw_hash,
            strict_dhash=entry.strict_dhash,
            summary_text=entry.summary_text,
            kind=entry.kind,
            is_custom_emoji=entry.is_custom_emoji,
        )

    @staticmethod
    def make_key(*, raw_hash: str, strict_dhash: str) -> str:
        left = raw_hash.strip()
        right = strict_dhash.strip()
        if left and right:
            return f"{left}:{right}"
        return left or right

    def _load(self) -> None:
        if self._path is None or not self._path.exists():
            return
        try:
            payload = json.loads(self._path.read_text(encoding="utf-8"))
        except Exception:
            return
        if not isinstance(payload, dict):
            return
        entries = payload.get("entries", {})
        if not isinstance(entries, dict):
            return
        self._entries = {
            str(key): ImageSummaryEntry.from_dict(value)
            for key, value in entries.items()
            if isinstance(value, dict)
        }

    def _save(self) -> None:
        if self._path is None:
            return
        payload = {
            "entries": {key: value.to_dict() for key, value in self._entries.items()},
        }
        tmp = self._path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, self._path)
