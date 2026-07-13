"""Bounded canonical JSON-object validation for durable persistence."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any

MAX_CANONICAL_JSON_BYTES = 1_048_576
MAX_CANONICAL_JSON_DEPTH = 128
MAX_CANONICAL_JSON_NODES = 65_536


class DuplicateJSONKeyError(ValueError):
    """Raised when a JSON object repeats one member name."""


@dataclass(slots=True, frozen=True)
class CanonicalJSONObjectValidation:
    """Parsed JSON object plus stable persistence violations."""

    payload: dict[str, Any] | None
    canonical_json: str | None
    violations: tuple[str, ...]


def validate_canonical_json_object(value: str) -> CanonicalJSONObjectValidation:
    """Validate one bounded, duplicate-free, canonical JSON object."""

    try:
        encoded = value.encode("utf-8", errors="strict")
    except UnicodeEncodeError:
        return _invalid("payload_json_invalid_utf8")
    if len(encoded) > MAX_CANONICAL_JSON_BYTES:
        return _invalid("payload_json_too_large")
    if _json_nesting_exceeds(value, MAX_CANONICAL_JSON_DEPTH):
        return _invalid("payload_json_too_deep")
    try:
        loaded = json.loads(
            value,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_nonstandard_constant,
        )
    except DuplicateJSONKeyError:
        return _invalid("payload_json_duplicate_key")
    except (RecursionError, TypeError, ValueError, json.JSONDecodeError):
        return _invalid("payload_json_invalid")
    if not isinstance(loaded, dict):
        return _invalid("payload_json_not_object")
    traversal_violation = _bounded_json_tree_violation(loaded)
    if traversal_violation:
        return _invalid(traversal_violation)
    try:
        canonical = json.dumps(
            loaded,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        canonical.encode("utf-8", errors="strict")
    except (RecursionError, UnicodeEncodeError, ValueError):
        return _invalid("payload_json_invalid_utf8")
    violations = () if value == canonical else ("payload_json_noncanonical",)
    return CanonicalJSONObjectValidation(
        payload=loaded,
        canonical_json=canonical,
        violations=violations,
    )


def _invalid(violation: str) -> CanonicalJSONObjectValidation:
    return CanonicalJSONObjectValidation(
        payload=None,
        canonical_json=None,
        violations=(violation,),
    )


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, item in pairs:
        if key in result:
            raise DuplicateJSONKeyError(key)
        result[key] = item
    return result


def _reject_nonstandard_constant(value: str) -> None:
    raise ValueError(f"non-standard JSON constant: {value}")


def _json_nesting_exceeds(value: str, maximum: int) -> bool:
    depth = 0
    in_string = False
    escaped = False
    for character in value:
        if in_string:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_string = False
            continue
        if character == '"':
            in_string = True
        elif character in "[{":
            depth += 1
            if depth > maximum:
                return True
        elif character in "]}":
            depth = max(0, depth - 1)
    return False


def _bounded_json_tree_violation(root: object) -> str:
    stack: list[tuple[object, int]] = [(root, 1)]
    nodes = 0
    while stack:
        value, depth = stack.pop()
        nodes += 1
        if nodes > MAX_CANONICAL_JSON_NODES:
            return "payload_json_too_many_nodes"
        if depth > MAX_CANONICAL_JSON_DEPTH:
            return "payload_json_too_deep"
        if isinstance(value, float) and not math.isfinite(value):
            return "payload_json_nonfinite"
        if isinstance(value, str):
            try:
                value.encode("utf-8", errors="strict")
            except UnicodeEncodeError:
                return "payload_json_invalid_utf8"
            continue
        if isinstance(value, dict):
            for key, item in value.items():
                try:
                    key.encode("utf-8", errors="strict")
                except UnicodeEncodeError:
                    return "payload_json_invalid_utf8"
                stack.append((item, depth + 1))
        elif isinstance(value, list):
            stack.extend((item, depth + 1) for item in value)
    return ""


__all__ = [
    "CanonicalJSONObjectValidation",
    "MAX_CANONICAL_JSON_BYTES",
    "MAX_CANONICAL_JSON_DEPTH",
    "MAX_CANONICAL_JSON_NODES",
    "validate_canonical_json_object",
]
