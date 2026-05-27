"""Shared stateless utilities for agent runners and workflows."""

from __future__ import annotations

import json
from typing import Any


def parse_json_object(text: str) -> dict[str, Any] | None:
    """Parse a JSON object, tolerating simple fenced-code responses."""

    candidate = text.strip()
    if candidate.startswith("```"):
        lines = candidate.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        candidate = "\n".join(lines).strip()
    try:
        payload = json.loads(candidate)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def json_schema_response_format(
    name: str,
    properties: dict[str, Any],
    required: list[str],
) -> dict[str, Any]:
    """Build a JSON schema response format configuration for structured output.

    Args:
        name: The schema name used by the model provider.
        properties: JSON Schema property definitions.
        required: List of required property names.

    Returns:
        A dict suitable for passing as a ``response_format`` parameter.
    """
    return {
        "type": "json_schema",
        "json_schema": {
            "name": name,
            "schema": {
                "type": "object",
                "properties": properties,
                "required": required,
                "additionalProperties": False,
            },
        },
    }


def instance_id_from_session(session_id: str) -> str:
    """Extract the instance ID prefix from a colon-separated session ID.

    Args:
        session_id: A session ID in the form ``"instance_id:..."``.

    Returns:
        The instance ID portion, or an empty string if no colon separator exists.
    """
    return session_id.split(":", 1)[0] if ":" in session_id else ""


def int_list(value: Any) -> list[int]:
    """Convert a list of values to a list of integers, ignoring non-convertible items.
    Args:
        value: A list of values to convert.

    Returns:
        A list of successfully converted integers.
    """
    if not isinstance(value, list):
        return []
    result: list[int] = []
    for item in value:
        item_int = optional_int(item)
        if item_int is not None:
            result.append(item_int)
    return result


def optional_int(value: Any) -> int | None:
    """Attempt to convert a value to an integer, returning ``None`` if not possible.
    Booleans are explicitly excluded (``isinstance(True, int)`` is ``True`` in
    Python, but ``True``/``False`` are not considered valid integer values here.
    Args:
        value: The value to convert.

    Returns:
        The integer value, or ``None`` if conversion is not applicable.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None
