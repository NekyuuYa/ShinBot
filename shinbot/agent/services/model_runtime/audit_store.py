"""File-backed model execution request/response audit payloads."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import logging
import os
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from shinbot.agent.services.model_runtime.extraction import (
    DATA_URL_PREFIX,
    MEDIA_SHA256_REF_PREFIX,
    sanitize_messages_for_audit,
)

logger = logging.getLogger(__name__)

MODEL_AUDIT_DIRNAME = "model-audit"
DEFAULT_AUDIT_PAYLOAD_TTL_SECONDS = 7 * 24 * 60 * 60
AUDIT_PAYLOAD_SCHEMA_VERSION = 1

_SAFE_EXECUTION_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_REDACTED_KEYS = {
    "api_key",
    "api_token",
    "access_token",
    "authorization",
    "Authorization",
    "app_secret",
    "api_secret",
}
_BINARY_OR_VECTOR_KEYS = {
    "b64_json",
    "base64",
    "image_base64",
    "audio",
    "audio_bytes",
    "bytes",
    "embedding",
    "embeddings",
}


class ModelAuditPayloadStore:
    """Persist detailed model audit payloads outside SQLite."""

    def __init__(
        self,
        data_dir: Path | str,
        *,
        ttl_seconds: int = DEFAULT_AUDIT_PAYLOAD_TTL_SECONDS,
    ) -> None:
        self.root = Path(data_dir) / MODEL_AUDIT_DIRNAME
        self.ttl_seconds = ttl_seconds

    def write(
        self,
        *,
        execution_id: str,
        created_at: datetime,
        payload: dict[str, Any],
    ) -> dict[str, str] | None:
        """Write a sanitized audit payload to disk.

        Args:
            execution_id: Unique execution identifier used as the filename stem.
            created_at: Timestamp when the execution was created.
            payload: Raw request/response payload to persist.

        Returns:
            A dict with ``audit_payload_ref`` and ``audit_payload_expires_at``
            keys on success, or ``None`` if the id is unsafe.
        """
        if not _is_safe_execution_id(execution_id):
            logger.warning("Skip model audit payload with unsafe execution id %r", execution_id)
            return None

        expires_at = created_at + timedelta(seconds=self.ttl_seconds)
        document = {
            "schema_version": AUDIT_PAYLOAD_SCHEMA_VERSION,
            "execution_id": execution_id,
            "created_at": created_at.isoformat(),
            "expires_at": expires_at.isoformat(),
            **sanitize_payload_for_audit(payload),
        }
        self.root.mkdir(parents=True, exist_ok=True)
        path = self._path_for(execution_id)
        tmp_path = path.with_suffix(f".{os.getpid()}.tmp")
        tmp_path.write_text(
            json.dumps(document, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp_path.replace(path)
        self.cleanup_expired(now=created_at)
        return {
            "audit_payload_ref": f"{MODEL_AUDIT_DIRNAME}/{path.name}",
            "audit_payload_expires_at": expires_at.isoformat(),
        }

    def read(self, execution_id: str) -> dict[str, Any] | None:
        """Read a previously written audit payload by execution id.

        Args:
            execution_id: The execution identifier to look up.

        Returns:
            The parsed payload dict, or ``None`` if missing, expired, or unreadable.
        """
        if not _is_safe_execution_id(execution_id):
            return None
        path = self._path_for(execution_id)
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            logger.exception("Failed to read model audit payload %s", path)
            return None
        if _is_expired(payload.get("expires_at"), now=datetime.now(UTC)):
            _unlink_quietly(path)
            return None
        return payload if isinstance(payload, dict) else None

    def cleanup_expired(self, *, now: datetime | None = None) -> int:
        """Remove expired audit payload files from disk.

        Args:
            now: Reference timestamp for expiry comparison. Defaults to the
                current UTC time when ``None``.

        Returns:
            The number of expired files deleted.
        """
        if not self.root.exists():
            return 0
        current = now or datetime.now(UTC)
        ttl_seconds = max(self.ttl_seconds, 1)
        # Fast path: skip files whose mtime is still inside the TTL window.
        # A full stat+parse of every file was the root cause of the event-loop
        # pinning (~16k files × ~8 MB each parsed on every model call).
        cutoff_epoch = current.timestamp() - ttl_seconds
        deleted = 0
        for path in self.root.glob("*.json"):
            try:
                if path.stat().st_mtime > cutoff_epoch:
                    continue
                payload = json.loads(path.read_text(encoding="utf-8"))
                if _is_expired(payload.get("expires_at"), now=current):
                    _unlink_quietly(path)
                    deleted += 1
            except OSError:
                continue
        return deleted

    def _path_for(self, execution_id: str) -> Path:
        return self.root / f"{execution_id}.json"


def sanitize_payload_for_audit(payload: Any) -> Any:
    """Return a JSON-safe payload with secrets and inline media removed."""

    return _sanitize_value(payload)


def _sanitize_value(value: Any, *, key: str = "") -> Any:
    if isinstance(value, dict):
        if value.get("type") == "image_url":
            return sanitize_messages_for_audit([{"role": "user", "content": [value]}])[0][
                "content"
            ][0]
        return {
            str(item_key): _sanitize_value(item_value, key=str(item_key))
            for item_key, item_value in value.items()
        }
    if isinstance(value, list):
        if key in _BINARY_OR_VECTOR_KEYS:
            return _redacted_sequence(value, key=key)
        return [_sanitize_value(item, key=key) for item in value]
    if isinstance(value, tuple):
        return [_sanitize_value(item, key=key) for item in value]
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"redacted": True, "type": "bytes", "byte_size": len(value)}
    if isinstance(value, str):
        if key in _REDACTED_KEYS and value:
            return "***"
        if value.startswith(DATA_URL_PREFIX):
            return _data_url_reference(value)
        if key in _BINARY_OR_VECTOR_KEYS:
            return _redacted_blob(value, key=key)
        return value
    if isinstance(value, int | float | bool) or value is None:
        return value
    return repr(value)


def _redacted_sequence(value: list[Any], *, key: str) -> dict[str, Any]:
    return {
        "redacted": True,
        "type": key,
        "item_count": len(value),
    }


def _redacted_blob(value: str, *, key: str) -> dict[str, Any]:
    digest = hashlib.sha256(value.encode("utf-8", errors="replace")).hexdigest()
    return {
        "redacted": True,
        "type": key,
        "char_count": len(value),
        "sha256": digest,
    }


def _data_url_reference(url: str) -> dict[str, Any]:
    header, separator, encoded_payload = url.partition(",")
    media_type = header[len(DATA_URL_PREFIX) :].split(";", 1)[0] or "application/octet-stream"
    if not separator:
        return {
            "url": "data-url:redacted",
            "source": "data_url",
            "mime_type": media_type,
            "encoded_chars": 0,
            "byte_size": 0,
            "redacted": True,
            "decode_error": "missing_payload",
        }
    if ";base64" not in header.lower():
        raw = encoded_payload.encode("utf-8", errors="replace")
        digest = hashlib.sha256(raw).hexdigest()
        return _media_reference(
            digest=digest,
            media_type=media_type,
            byte_size=len(raw),
            encoded_chars=len(encoded_payload),
            encoding="plain",
        )
    try:
        raw = base64.b64decode(encoded_payload, validate=True)
    except (binascii.Error, ValueError):
        return {
            "url": "data-url:redacted",
            "source": "data_url",
            "mime_type": media_type,
            "encoded_chars": len(encoded_payload),
            "byte_size": _estimate_base64_size(encoded_payload),
            "redacted": True,
            "encoding": "base64",
            "decode_error": "invalid_base64",
        }
    digest = hashlib.sha256(raw).hexdigest()
    return _media_reference(
        digest=digest,
        media_type=media_type,
        byte_size=len(raw),
        encoded_chars=len(encoded_payload),
        encoding="base64",
    )


def _media_reference(
    *,
    digest: str,
    media_type: str,
    byte_size: int,
    encoded_chars: int,
    encoding: str,
) -> dict[str, Any]:
    return {
        "url": f"{MEDIA_SHA256_REF_PREFIX}{digest}",
        "source": "data_url",
        "mime_type": media_type,
        "raw_hash": digest,
        "byte_size": byte_size,
        "encoded_chars": encoded_chars,
        "redacted": True,
        "encoding": encoding,
    }


def _estimate_base64_size(payload: str) -> int:
    compact = "".join(payload.split())
    if not compact:
        return 0
    padding = len(compact) - len(compact.rstrip("="))
    return max(0, (len(compact) * 3) // 4 - padding)


def _is_expired(value: Any, *, now: datetime) -> bool:
    if not isinstance(value, str) or not value:
        return False
    try:
        expires_at = datetime.fromisoformat(value)
    except ValueError:
        return False
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=UTC)
    return expires_at <= now


def _is_safe_execution_id(value: str) -> bool:
    return bool(_SAFE_EXECUTION_ID.fullmatch(value))


def _unlink_quietly(path: Path) -> None:
    try:
        path.unlink()
    except OSError:
        logger.exception("Failed to remove expired model audit payload %s", path)
