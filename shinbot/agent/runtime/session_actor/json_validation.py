"""Bounded JSON-tree validation for durable Session Actor declarations."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping

MAX_DURABLE_JSON_BYTES = 1_048_576
MAX_DURABLE_JSON_DEPTH = 128
MAX_DURABLE_JSON_NODES = 65_536


class DurableJSONValidationError(ValueError):
    """Raised when a durable declaration is not bounded UTF-8 JSON."""


def validate_durable_json(value: object, *, path: str) -> None:
    """Validate a JSON-compatible tree without recursive traversal.

    The limits intentionally match persistence canonical-JSON validation so an
    actor declaration cannot enter the store with a shape that recovery would
    later reject.

    Args:
        value: Candidate in-memory JSON value.
        path: Root field name used in validation errors.

    Raises:
        DurableJSONValidationError: If the value is malformed, unbounded, or
            contains text that cannot be encoded as strict UTF-8.
    """

    stack: list[tuple[object, str, int]] = [(value, path, 1)]
    node_count = 0
    encoded_size = 0
    while stack:
        current, current_path, depth = stack.pop()
        node_count += 1
        if node_count > MAX_DURABLE_JSON_NODES:
            raise DurableJSONValidationError(
                f"{path} exceeds the maximum JSON node count"
            )
        if depth > MAX_DURABLE_JSON_DEPTH:
            raise DurableJSONValidationError(
                f"{path} exceeds the maximum JSON depth"
            )

        if isinstance(current, Mapping):
            encoded_size += 2 + max(0, len(current) - 1) + len(current)
            _require_container_capacity(
                path=path,
                node_count=node_count,
                pending_count=len(stack),
                child_count=len(current),
            )
            _require_encoded_capacity(encoded_size, path=path)
            for key, item in current.items():
                if not isinstance(key, str):
                    raise DurableJSONValidationError(
                        f"{current_path} keys must be JSON strings"
                    )
                encoded_size += _encoded_scalar_size(
                    key,
                    path=f"{current_path} key",
                )
                _require_encoded_capacity(encoded_size, path=path)
                stack.append((item, f"{current_path}.{key}", depth + 1))
        elif isinstance(current, (list, tuple)):
            encoded_size += 2 + max(0, len(current) - 1)
            _require_container_capacity(
                path=path,
                node_count=node_count,
                pending_count=len(stack),
                child_count=len(current),
            )
            _require_encoded_capacity(encoded_size, path=path)
            stack.extend(
                (item, f"{current_path}[{index}]", depth + 1)
                for index, item in enumerate(current)
            )
        elif isinstance(current, float):
            if not math.isfinite(current):
                raise DurableJSONValidationError(
                    f"{current_path} numbers must be finite"
                )
            encoded_size += _encoded_scalar_size(current, path=current_path)
        elif current is None or isinstance(current, (str, int, bool)):
            encoded_size += _encoded_scalar_size(current, path=current_path)
        else:
            raise DurableJSONValidationError(
                f"{current_path} must contain only JSON-compatible values"
            )

        _require_encoded_capacity(encoded_size, path=path)


def _encoded_scalar_size(value: object, *, path: str) -> int:
    try:
        rendered = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        )
        return len(rendered.encode("utf-8", errors="strict"))
    except (RecursionError, TypeError, UnicodeEncodeError, ValueError) as exc:
        raise DurableJSONValidationError(
            f"{path} must contain valid bounded UTF-8 JSON"
        ) from exc


def _require_container_capacity(
    *,
    path: str,
    node_count: int,
    pending_count: int,
    child_count: int,
) -> None:
    if node_count + pending_count + child_count > MAX_DURABLE_JSON_NODES:
        raise DurableJSONValidationError(
            f"{path} exceeds the maximum JSON node count"
        )


def _require_encoded_capacity(encoded_size: int, *, path: str) -> None:
    if encoded_size > MAX_DURABLE_JSON_BYTES:
        raise DurableJSONValidationError(
            f"{path} exceeds the maximum encoded JSON size"
        )


__all__ = [
    "DurableJSONValidationError",
    "MAX_DURABLE_JSON_BYTES",
    "MAX_DURABLE_JSON_DEPTH",
    "MAX_DURABLE_JSON_NODES",
    "validate_durable_json",
]
