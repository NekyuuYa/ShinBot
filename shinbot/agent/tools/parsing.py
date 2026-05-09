"""Shared tool-call parsing utilities."""

from __future__ import annotations

import json
from typing import Any


def parse_tool_call(tool_call: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
    """Extract (tool_call_id, tool_name, arguments) from a raw tool-call dict."""
    tool_call_id = str(tool_call.get("id", "") or "")
    function = tool_call.get("function", {}) if isinstance(tool_call, dict) else {}
    tool_name = str(function.get("name", "") or "")
    raw_arguments = function.get("arguments", "{}")
    try:
        arguments = (
            json.loads(raw_arguments)
            if isinstance(raw_arguments, str)
            else dict(raw_arguments or {})
        )
    except (json.JSONDecodeError, TypeError, ValueError):
        arguments = {}
    return tool_call_id, tool_name, arguments


__all__ = ["parse_tool_call"]
