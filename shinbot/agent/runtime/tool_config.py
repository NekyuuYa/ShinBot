"""Runtime tool-extension config shared by Agent runners."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(slots=True, frozen=True)
class StageToolConfig:
    """Additional tool exposure for one runner stage.

    Built-in workflow tools are owned by the runner and remain hard-coded.
    This config only adds extra tools by name or by registry tag.
    """

    extra_names: tuple[str, ...] = ()
    extra_tags: tuple[str, ...] = ()


def stage_tool_config_from_mapping(value: dict[str, Any] | None) -> StageToolConfig:
    """Build a ``StageToolConfig`` from an arbitrary mapping.

    Accepts legacy key aliases (``extra_names`` / ``names``,
    ``extra_tags``) and normalises them into the canonical
    ``extra_names`` and ``extra_tags`` tuple fields.

    Args:
        value: Raw configuration mapping, or ``None`` for defaults.

    Returns:
        A frozen ``StageToolConfig`` with deduplicated string tuples.
    """
    mapping = value if isinstance(value, dict) else {}
    return StageToolConfig(
        extra_names=_string_tuple(
            mapping.get("extra", mapping.get("extra_names", mapping.get("names")))
        ),
        extra_tags=_string_tuple(
            mapping.get("tags", mapping.get("extra_tags"))
        ),
    )


def build_configured_extra_tools(
    tool_manager: Any,
    *,
    config: StageToolConfig,
    caller: str,
    instance_id: str,
    session_id: str,
    user_id: str = "",
) -> list[dict[str, Any]]:
    """Build additive configured tool schemas, preserving registry order by tag."""

    tools: list[dict[str, Any]] = []
    if config.extra_names:
        tools.extend(
            tool_manager.build_request_tools(
                list(config.extra_names),
                caller=caller,
                instance_id=instance_id,
                session_id=session_id,
                user_id=user_id,
            )
        )
    for tag in config.extra_tags:
        tools.extend(
            tool_manager.export_model_tools(
                caller=caller,
                instance_id=instance_id,
                session_id=session_id,
                user_id=user_id,
                tags={tag},
            )
        )
    return tools


def merge_tool_schemas(
    *groups: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Merge tool schemas by function name while preserving first occurrence."""

    merged: list[dict[str, Any]] = []
    seen_names: set[str] = set()
    for group in groups:
        for tool in group:
            name = _tool_schema_name(tool)
            if not name or name in seen_names:
                continue
            seen_names.add(name)
            merged.append(tool)
    return merged


def _tool_schema_name(tool: dict[str, Any]) -> str:
    function = tool.get("function")
    if not isinstance(function, dict):
        return ""
    return str(function.get("name") or "").strip()


def _string_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        values = (value,)
    elif isinstance(value, (list, tuple)):
        values = value
    else:
        return ()
    result: list[str] = []
    seen: set[str] = set()
    for item in values:
        normalized = str(item or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return tuple(result)


__all__ = [
    "StageToolConfig",
    "build_configured_extra_tools",
    "merge_tool_schemas",
    "stage_tool_config_from_mapping",
]
