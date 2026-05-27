"""File-backed instance runtime configuration repository."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING, Any

from shinbot.persistence.records import InstanceConfigRecord, utc_now_iso

if TYPE_CHECKING:
    from shinbot.persistence.engine import DatabaseManager


INSTANCE_CONFIGS_FILE_VERSION = 1
INSTANCE_CONFIGS_FILENAME = "instance-configs.json"


class InstanceConfigRepositoryError(ValueError):
    """Raised when the file-backed instance config registry is invalid."""


class InstanceConfigRepository:
    """Per-instance runtime configuration stored as a single editable JSON file."""

    def __init__(self, source: DatabaseManager | Path | str) -> None:
        """Initialise the repository.

        Args:
            source: A ``DatabaseManager``, ``Path``, or string path to the
                JSON config file or its parent directory.
        """
        self.path = _resolve_instance_configs_path(source)

    @classmethod
    def from_data_dir(cls, data_dir: Path | str) -> InstanceConfigRepository:
        """Create a repository pointing at *data_dir*/instance-configs.json."""
        return cls(Path(data_dir) / INSTANCE_CONFIGS_FILENAME)

    def ensure_file(self) -> Path:
        """Create the JSON file with an empty payload when it does not exist.

        Returns:
            Path to the instance configs file.
        """
        if not self.path.exists():
            self._write_payload(_empty_payload())
        return self.path

    def list(self) -> list[dict[str, Any]]:
        """Return all instance configurations sorted by (instance_id, uuid)."""
        payload = self._read_payload()
        configs = [_normalize_config(item) for item in payload["configs"]]
        configs.sort(key=lambda item: (item["instance_id"], item["uuid"]))
        return configs

    def get(self, config_uuid: str) -> dict[str, Any] | None:
        """Return a configuration by UUID, or ``None`` if not found.

        Args:
            config_uuid: UUID of the instance configuration.
        """
        payload = self._read_payload()
        item = _find_by_uuid(payload["configs"], config_uuid)
        return _normalize_config(item) if item is not None else None

    def get_by_instance_id(self, instance_id: str) -> dict[str, Any] | None:
        """Return a configuration by instance ID, or ``None``.

        Args:
            instance_id: Platform-level instance identifier.
        """
        payload = self._read_payload()
        item = _find_by_instance_id(payload["configs"], instance_id)
        return _normalize_config(item) if item is not None else None

    def upsert(self, record: InstanceConfigRecord) -> None:
        """Insert or update an instance configuration.

        Args:
            record: The configuration record to persist.

        Raises:
            InstanceConfigRepositoryError: If the instance_id is already
                claimed by a different UUID.
        """
        payload = self._read_payload()
        incoming = _config_from_record(record)
        same_instance = _find_by_instance_id(payload["configs"], incoming["instance_id"])
        if same_instance is not None and same_instance.get("uuid") != incoming["uuid"]:
            raise InstanceConfigRepositoryError(
                f"Instance config for {incoming['instance_id']!r} already exists"
            )

        existing = _find_by_uuid(payload["configs"], incoming["uuid"])
        if existing is not None:
            incoming["created_at"] = existing.get("created_at") or incoming["created_at"]
            existing.clear()
            existing.update(incoming)
        else:
            payload["configs"].append(incoming)
        self._write_payload(payload)

    def delete(self, config_uuid: str) -> None:
        """Remove an instance configuration by UUID.

        Args:
            config_uuid: UUID of the configuration to delete.
        """
        payload = self._read_payload()
        payload["configs"] = [
            item for item in payload["configs"] if str(item.get("uuid")) != config_uuid
        ]
        self._write_payload(payload)

    def _read_payload(self) -> dict[str, Any]:
        if not self.path.exists():
            return _empty_payload()
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise InstanceConfigRepositoryError(f"Invalid instance config JSON: {exc}") from exc
        if not isinstance(raw, Mapping):
            raise InstanceConfigRepositoryError("Instance config root must be an object")
        return _normalize_payload(raw)

    def _write_payload(self, payload: dict[str, Any]) -> None:
        normalized = _normalize_payload(payload)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.path.with_name(f".{self.path.name}.tmp")
        temp_path.write_text(
            json.dumps(normalized, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        temp_path.replace(self.path)


def _resolve_instance_configs_path(source: DatabaseManager | Path | str) -> Path:
    if isinstance(source, (Path, str)):
        return Path(source)
    config = source.config
    data_dir = Path(getattr(config, "data_dir", config.sqlite_path.parent.parent))
    return data_dir / INSTANCE_CONFIGS_FILENAME


def _empty_payload() -> dict[str, Any]:
    return {
        "version": INSTANCE_CONFIGS_FILE_VERSION,
        "configs": [],
    }


def _normalize_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "version": INSTANCE_CONFIGS_FILE_VERSION,
        "configs": [_normalize_config(item) for item in _list_of_maps(payload.get("configs"))],
    }


def _list_of_maps(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _find_by_uuid(items: list[dict[str, Any]], config_uuid: str) -> dict[str, Any] | None:
    for item in items:
        if str(item.get("uuid")) == config_uuid:
            return item
    return None


def _find_by_instance_id(items: list[dict[str, Any]], instance_id: str) -> dict[str, Any] | None:
    for item in items:
        if str(item.get("instance_id")) == instance_id:
            return item
    return None


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _normalize_string(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value).strip()


def _normalize_tags(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _normalize_config(payload: Mapping[str, Any]) -> dict[str, Any]:
    now = utc_now_iso()
    return {
        "uuid": _normalize_string(payload.get("uuid")),
        "instance_id": _normalize_string(payload.get("instance_id")),
        "main_llm": _normalize_string(payload.get("main_llm")),
        "config": _mapping(payload.get("config")),
        "tags": _normalize_tags(payload.get("tags")),
        "created_at": _normalize_string(payload.get("created_at"), now),
        "updated_at": _normalize_string(payload.get("updated_at"), now),
    }


def _config_from_record(record: InstanceConfigRecord) -> dict[str, Any]:
    return _normalize_config(asdict(record))


__all__ = [
    "INSTANCE_CONFIGS_FILENAME",
    "INSTANCE_CONFIGS_FILE_VERSION",
    "InstanceConfigRepository",
    "InstanceConfigRepositoryError",
]
