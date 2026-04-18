"""File-based identity store for user-visible nickname mapping."""

from __future__ import annotations

import json
import re
import threading
from pathlib import Path
from typing import Any

_NOISE_CHARS_RE = re.compile(r"[^0-9A-Za-z\u4e00-\u9fff _-]+")
_MULTI_SPACE_RE = re.compile(r"\s+")
_DEFAULT_PAYLOAD = {"platform": "", "users": []}


class IdentityStore:
    """Manage identity mapping in a user-editable JSON file.

    The file keeps a single platform and a list of user identity entries.
    Users can edit this file directly to correct naming decisions.
    """

    def __init__(self, file_path: Path | str) -> None:
        self._file_path = Path(file_path)
        self._lock = threading.Lock()
        self._ensure_file_exists()

    @property
    def file_path(self) -> Path:
        return self._file_path

    @staticmethod
    def sanitize_name(raw_name: str) -> str:
        """Remove emoji/noise symbols and normalize whitespace."""
        text = str(raw_name or "").strip()
        if not text:
            return ""
        cleaned = _NOISE_CHARS_RE.sub("", text)
        cleaned = _MULTI_SPACE_RE.sub(" ", cleaned).strip(" _-")
        return cleaned[:48]

    def get_identity(self, user_id: str, *, platform: str = "") -> dict[str, Any] | None:
        normalized_user_id = str(user_id).strip()
        if not normalized_user_id:
            return None
        identities = self.list_identities(platform=platform)
        return identities.get(normalized_user_id)

    def list_identities(self, *, platform: str = "") -> dict[str, dict[str, Any]]:
        payload = self._load_payload()
        stored_platform = str(payload.get("platform", "")).strip()
        normalized_platform = str(platform).strip()
        if normalized_platform and stored_platform and stored_platform != normalized_platform:
            return {}

        result: dict[str, dict[str, Any]] = {}
        for item in self._normalize_users(payload.get("users")):
            result[item["user_id"]] = item
        return result

    def ensure_user(
        self,
        *,
        user_id: str,
        suggested_name: str = "",
        platform: str = "",
    ) -> dict[str, Any] | None:
        """Ensure a user exists in identities.json, respecting locked entries."""
        normalized_user_id = str(user_id).strip()
        if not normalized_user_id:
            return None

        normalized_platform = str(platform).strip()
        cleaned_name = self.sanitize_name(suggested_name)

        with self._lock:
            payload = self._load_payload()
            stored_platform = str(payload.get("platform", "")).strip()

            if normalized_platform and not stored_platform:
                payload["platform"] = normalized_platform
                stored_platform = normalized_platform

            users = self._normalize_users(payload.get("users"))
            target = next((item for item in users if item["user_id"] == normalized_user_id), None)
            changed = False

            if target is None:
                target = {
                    "user_id": normalized_user_id,
                    "name": cleaned_name or f"user_{normalized_user_id[-6:]}",
                    "aname": [],
                    "note": "",
                    "locked": False,
                }
                users.append(target)
                changed = True
            elif (
                not bool(target.get("locked"))
                and cleaned_name
                and cleaned_name != target.get("name", "")
            ):
                target["name"] = cleaned_name
                changed = True

            if changed:
                payload["users"] = users
                self._write_payload(payload)

            # Keep behavior deterministic for mixed platforms when a single file is used.
            if normalized_platform and stored_platform and stored_platform != normalized_platform:
                return None

            return dict(target)

    def _ensure_file_exists(self) -> None:
        self._file_path.parent.mkdir(parents=True, exist_ok=True)
        if self._file_path.exists():
            return
        self._write_payload(dict(_DEFAULT_PAYLOAD))

    def _load_payload(self) -> dict[str, Any]:
        self._ensure_file_exists()
        try:
            raw = self._file_path.read_text(encoding="utf-8")
        except OSError:
            return dict(_DEFAULT_PAYLOAD)

        if not raw.strip():
            return dict(_DEFAULT_PAYLOAD)

        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return dict(_DEFAULT_PAYLOAD)

        if not isinstance(parsed, dict):
            return dict(_DEFAULT_PAYLOAD)

        payload = dict(parsed)
        payload["platform"] = str(payload.get("platform", "")).strip()
        payload["users"] = self._normalize_users(payload.get("users"))
        return payload

    def _write_payload(self, payload: dict[str, Any]) -> None:
        normalized = {
            "platform": str(payload.get("platform", "")).strip(),
            "users": self._normalize_users(payload.get("users")),
        }
        content = json.dumps(normalized, ensure_ascii=False, indent=2)
        self._file_path.write_text(content + "\n", encoding="utf-8")

    def _normalize_users(self, users: Any) -> list[dict[str, Any]]:
        if not isinstance(users, list):
            return []

        normalized_users: list[dict[str, Any]] = []
        seen_user_ids: set[str] = set()
        for item in users:
            if not isinstance(item, dict):
                continue
            user_id = str(item.get("user_id", "")).strip()
            if not user_id or user_id in seen_user_ids:
                continue
            seen_user_ids.add(user_id)

            aliases_raw = item.get("aname", item.get("aliases", []))
            if isinstance(aliases_raw, str):
                aliases = [aliases_raw.strip()] if aliases_raw.strip() else []
            elif isinstance(aliases_raw, list):
                aliases = [str(alias).strip() for alias in aliases_raw if str(alias).strip()]
            else:
                aliases = []

            normalized_users.append(
                {
                    "user_id": user_id,
                    "name": self.sanitize_name(str(item.get("name", ""))) or f"user_{user_id[-6:]}",
                    "aname": aliases,
                    "note": str(item.get("note", "")).strip(),
                    "locked": bool(item.get("locked", False)),
                }
            )

        return normalized_users
