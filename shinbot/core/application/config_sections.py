"""Helpers for reading normalized and legacy runtime config sections."""

from __future__ import annotations

import time
from typing import Any

ADAPTER_INSTANCES_SECTION = "adapter_instances"


def adapter_instance_store(
    config: dict[str, Any],
    *,
    create: bool = False,
) -> tuple[str, list[dict[str, Any]]]:
    """Return the active adapter-instance section.

    New configs use ``adapter_instances``. If absent, writes create it.
    """

    section = ADAPTER_INSTANCES_SECTION
    records = config.get(section)
    if isinstance(records, list):
        return section, records
    if create:
        records = []
        config[section] = records
        return section, records
    return section, []


def iter_adapter_instance_records(config: dict[str, Any]) -> list[dict[str, Any]]:
    """Return configured adapter instances from the active section."""

    _section, records = adapter_instance_store(config)
    return [item for item in records if isinstance(item, dict)]


def normalize_adapter_instance_record(item: dict[str, Any]) -> dict[str, Any]:
    """Normalize an adapter-instance record to API/runtime keys."""

    instance_id = str(item.get("id") or "")
    adapter = str(item.get("adapter") or "")
    config = item.get("config", {})
    if not isinstance(config, dict):
        config = {}

    return {
        "id": instance_id,
        "name": str(item.get("name") or instance_id),
        "adapter": adapter,
        "enabled": normalize_enabled(item.get("enabled", True)),
        "config": dict(config),
        "createdAt": item.get("createdAt", 0),
        "lastModified": item.get("lastModified", 0),
    }


def adapter_instance_storage_record(
    record: dict[str, Any],
    *,
    section: str,
) -> dict[str, Any]:
    """Convert a normalized/API record back to the target config section shape."""

    normalized = normalize_adapter_instance_record(record)
    payload: dict[str, Any] = {
        "id": normalized["id"],
        "adapter": normalized["adapter"],
        "enabled": normalized["enabled"],
        "config": normalized["config"],
    }
    if normalized["name"] and normalized["name"] != normalized["id"]:
        payload["name"] = normalized["name"]
    if normalized["createdAt"]:
        payload["createdAt"] = normalized["createdAt"]
    if normalized["lastModified"]:
        payload["lastModified"] = normalized["lastModified"]
    return payload


def set_adapter_instance_platform(record: dict[str, Any], platform: str) -> None:
    """Set the adapter platform name on an instance record."""
    record["adapter"] = platform


def append_adapter_instance_record(
    config: dict[str, Any],
    record: dict[str, Any],
) -> tuple[int, dict[str, Any]]:
    """Append a new adapter-instance record to the config.

    Args:
        config: The top-level configuration dict.
        record: The adapter-instance record to append.

    Returns:
        A tuple of the new record's index and its storage representation.
    """
    section, records = adapter_instance_store(config, create=True)
    stored = adapter_instance_storage_record(record, section=section)
    records.append(stored)
    return len(records) - 1, stored


def replace_adapter_instance_record(
    config: dict[str, Any],
    index: int,
    record: dict[str, Any],
) -> dict[str, Any]:
    """Replace an existing adapter-instance record at the given index.

    Args:
        config: The top-level configuration dict.
        index: The zero-based index of the record to replace.
        record: The new adapter-instance record.

    Returns:
        The storage representation of the replaced record.
    """
    section, records = adapter_instance_store(config, create=True)
    stored = adapter_instance_storage_record(record, section=section)
    records[index] = stored
    return stored


def normalize_enabled(value: Any, *, default: bool = True) -> bool:
    """Normalize a boolean-like value to a Python bool.

    Accepts booleans, truthy/falsy strings (``"true"``, ``"yes"``, ``"1"``,
    ``"on"``, ``"enabled"`` and their negatives), and numeric values.  Falls
    back to *default* when the value is ``None`` or an unrecognized string.

    Args:
        value: The raw value to normalize.
        default: Fallback boolean when the value cannot be interpreted.

    Returns:
        The normalized boolean value.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on", "enabled"}:
            return True
        if normalized in {"0", "false", "no", "off", "disabled"}:
            return False
        return default
    return bool(value)


def timestamp_now() -> int:
    """Return the current Unix timestamp as an integer."""
    return int(time.time())
