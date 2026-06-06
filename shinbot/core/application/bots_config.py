"""Bot service-unit config parsing and validation."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from shinbot.core.config_provider import ConfigValidationIssue

AGENT_MODES = frozenset({"none", "simple", "full"})
WILDCARD = "*"
PLUGIN_WILDCARD = WILDCARD
SESSION_PATTERN_TYPES = frozenset({"group", "private"})


@dataclass(slots=True, frozen=True)
class BotCommandsConfig:
    """Command routing policy for one configured bot."""

    enabled: bool = True
    prefixes: tuple[str, ...] = ("/",)


@dataclass(slots=True, frozen=True)
class BotPluginsConfig:
    """Plugin routing policy for one configured bot."""

    enabled: bool = True
    enabled_plugins: tuple[str, ...] = (PLUGIN_WILDCARD,)
    disabled_plugins: tuple[str, ...] = ()


@dataclass(slots=True, frozen=True)
class BotAgentConfig:
    """Agent behavior policy for one configured bot."""

    mode: str = "none"
    config: str = ""


@dataclass(slots=True, frozen=True)
class BotBindingConfig:
    """A bot binding to one adapter instance and a set of sessions."""

    id: str
    adapter_instance_id: str
    session_patterns: tuple[str, ...]
    enabled: bool = True
    priority: int = 0


@dataclass(slots=True, frozen=True)
class BotServiceConfig:
    """Normalized service-unit config for one bot."""

    id: str
    display_name: str
    enabled: bool = True
    administrators: tuple[str, ...] = ()
    commands: BotCommandsConfig = field(default_factory=BotCommandsConfig)
    plugins: BotPluginsConfig = field(default_factory=BotPluginsConfig)
    agent: BotAgentConfig = field(default_factory=BotAgentConfig)
    bindings: tuple[BotBindingConfig, ...] = ()


class BotServiceConfigError(ValueError):
    """Raised when configured bot service units are invalid."""

    def __init__(self, issues: list[ConfigValidationIssue]) -> None:
        """Initialise the error with a list of validation issues.

        Args:
            issues: Validation issues that caused this error.
        """
        self.issues = list(issues)
        super().__init__(_format_issues(self.issues))


def load_bot_service_configs(
    config: dict[str, Any],
    *,
    data_dir: Path | str | None = None,
) -> tuple[BotServiceConfig, ...]:
    """Return normalized bot configs or raise on validation issues."""

    bots, issues = parse_bot_service_configs(config, data_dir=data_dir)
    if issues:
        raise BotServiceConfigError(list(issues))
    return bots


def validate_bot_service_configs(
    config: dict[str, Any],
    *,
    data_dir: Path | str | None = None,
) -> list[ConfigValidationIssue]:
    """Validate bot service-unit config without raising."""

    _bots, issues = parse_bot_service_configs(config, data_dir=data_dir)
    return list(issues)


def parse_bot_service_configs(
    config: dict[str, Any],
    *,
    data_dir: Path | str | None = None,
) -> tuple[tuple[BotServiceConfig, ...], tuple[ConfigValidationIssue, ...]]:
    """Parse the normalized ``[[bots]]`` section.

    This parser intentionally understands only the new bot-centered shape.
    Legacy per-adapter/per-plugin bot wiring is not mapped or inferred here.
    """

    issues: list[ConfigValidationIssue] = []
    adapter_ids = _collect_section_ids(config, "adapter_instances", issues)
    plugin_ids = _collect_section_ids(config, "plugins", issues)

    raw_bots = config.get("bots", [])
    if raw_bots is None:
        raw_bots = []
    if not isinstance(raw_bots, list):
        _add_issue(issues, "bots", "must be a list of bot tables", "type")
        return (), tuple(issues)

    bots: list[BotServiceConfig] = []
    seen_bot_ids: dict[str, str] = {}
    seen_binding_ids: dict[str, str] = {}
    root_data_dir = Path(data_dir) if data_dir is not None else None

    for bot_index, raw_bot in enumerate(raw_bots):
        path = f"bots[{bot_index}]"
        if not isinstance(raw_bot, dict):
            _add_issue(issues, path, "must be a bot table", "type")
            continue

        bot_id = _required_string(raw_bot, "id", path, issues)
        if bot_id:
            _check_unique(bot_id, f"{path}.id", seen_bot_ids, issues)
        display_name = _optional_string(raw_bot, "display_name", path, issues, default=bot_id)
        enabled = _optional_bool(raw_bot, "enabled", path, issues, default=True)
        administrators = _optional_string_list(
            raw_bot,
            "administrators",
            path,
            issues,
            default=(),
        )
        commands = _parse_commands(raw_bot.get("commands"), f"{path}.commands", issues)
        plugins = _parse_plugins(raw_bot.get("plugins"), f"{path}.plugins", plugin_ids, issues)
        agent = _parse_agent(raw_bot.get("agent"), f"{path}.agent", root_data_dir, issues)
        bindings = _parse_bindings(
            raw_bot.get("bindings"),
            f"{path}.bindings",
            adapter_ids,
            seen_binding_ids,
            issues,
        )

        if not bot_id:
            continue
        bots.append(
            BotServiceConfig(
                id=bot_id,
                display_name=display_name or bot_id,
                enabled=enabled,
                administrators=administrators,
                commands=commands,
                plugins=plugins,
                agent=agent,
                bindings=tuple(bindings),
            )
        )

    return tuple(bots), tuple(issues)


def _parse_commands(
    raw_commands: Any,
    path: str,
    issues: list[ConfigValidationIssue],
) -> BotCommandsConfig:
    if raw_commands is None:
        return BotCommandsConfig()
    if not isinstance(raw_commands, dict):
        _add_issue(issues, path, "must be a table", "type")
        return BotCommandsConfig()

    enabled = _optional_bool(raw_commands, "enabled", path, issues, default=True)
    prefixes = _optional_string_list(
        raw_commands,
        "prefixes",
        path,
        issues,
        default=("/",),
        non_empty=True,
    )
    return BotCommandsConfig(enabled=enabled, prefixes=prefixes)


def _parse_plugins(
    raw_plugins: Any,
    path: str,
    plugin_ids: set[str],
    issues: list[ConfigValidationIssue],
) -> BotPluginsConfig:
    if raw_plugins is None:
        return BotPluginsConfig()
    if not isinstance(raw_plugins, dict):
        _add_issue(issues, path, "must be a table", "type")
        return BotPluginsConfig()

    enabled = _optional_bool(raw_plugins, "enabled", path, issues, default=True)
    enabled_plugins = _optional_string_list(
        raw_plugins,
        "enabled_plugins",
        path,
        issues,
        default=(PLUGIN_WILDCARD,),
    )
    disabled_plugins = _optional_string_list(
        raw_plugins,
        "disabled_plugins",
        path,
        issues,
        default=(),
    )
    _validate_plugin_refs(enabled_plugins, f"{path}.enabled_plugins", plugin_ids, issues)
    _validate_plugin_refs(disabled_plugins, f"{path}.disabled_plugins", plugin_ids, issues)
    return BotPluginsConfig(
        enabled=enabled,
        enabled_plugins=enabled_plugins,
        disabled_plugins=disabled_plugins,
    )


def _parse_agent(
    raw_agent: Any,
    path: str,
    data_dir: Path | None,
    issues: list[ConfigValidationIssue],
) -> BotAgentConfig:
    if raw_agent is None:
        return BotAgentConfig()
    if not isinstance(raw_agent, dict):
        _add_issue(issues, path, "must be a table", "type")
        return BotAgentConfig()

    mode = _optional_string(raw_agent, "mode", path, issues, default="none").lower()
    if mode not in AGENT_MODES:
        _add_issue(issues, f"{path}.mode", "must be one of: none, simple, full", "choices")
        mode = "none"

    agent_config = _optional_string(raw_agent, "config", path, issues, default="")
    if mode == "full" and not agent_config:
        _add_issue(issues, f"{path}.config", "is required when mode is full", "required")
    if agent_config:
        _validate_data_relative_path(agent_config, f"{path}.config", data_dir, issues)
    return BotAgentConfig(mode=mode, config=agent_config)


def _parse_bindings(
    raw_bindings: Any,
    path: str,
    adapter_ids: set[str],
    seen_binding_ids: dict[str, str],
    issues: list[ConfigValidationIssue],
) -> list[BotBindingConfig]:
    if raw_bindings is None:
        return []
    if not isinstance(raw_bindings, list):
        _add_issue(issues, path, "must be a list of binding tables", "type")
        return []

    bindings: list[BotBindingConfig] = []
    for binding_index, raw_binding in enumerate(raw_bindings):
        binding_path = f"{path}[{binding_index}]"
        if not isinstance(raw_binding, dict):
            _add_issue(issues, binding_path, "must be a binding table", "type")
            continue

        if "session_pattern" in raw_binding:
            _add_issue(
                issues,
                f"{binding_path}.session_pattern",
                "use session_patterns = [...]",
                "deprecated",
            )

        binding_id = _required_string(raw_binding, "id", binding_path, issues)
        if binding_id:
            _check_unique(binding_id, f"{binding_path}.id", seen_binding_ids, issues)
        adapter_instance_id = _required_string(
            raw_binding,
            "adapter_instance_id",
            binding_path,
            issues,
        )
        if adapter_instance_id and adapter_instance_id not in adapter_ids:
            _add_issue(
                issues,
                f"{binding_path}.adapter_instance_id",
                f"unknown adapter instance: {adapter_instance_id}",
                "unknown_ref",
            )

        session_patterns = _required_string_list(
            raw_binding,
            "session_patterns",
            binding_path,
            issues,
            non_empty=True,
        )
        _validate_session_patterns(session_patterns, f"{binding_path}.session_patterns", issues)
        enabled = _optional_bool(raw_binding, "enabled", binding_path, issues, default=True)
        priority = _optional_int(raw_binding, "priority", binding_path, issues, default=0)

        if not binding_id or not adapter_instance_id or not session_patterns:
            continue
        bindings.append(
            BotBindingConfig(
                id=binding_id,
                adapter_instance_id=adapter_instance_id,
                session_patterns=session_patterns,
                enabled=enabled,
                priority=priority,
            )
        )
    return bindings


def _collect_section_ids(
    config: dict[str, Any],
    section: str,
    issues: list[ConfigValidationIssue],
) -> set[str]:
    raw_section = config.get(section, [])
    if raw_section is None:
        return set()
    if not isinstance(raw_section, list):
        _add_issue(issues, section, "must be a list of tables", "type")
        return set()

    result: set[str] = set()
    seen: dict[str, str] = {}
    for index, item in enumerate(raw_section):
        path = f"{section}[{index}]"
        if not isinstance(item, dict):
            _add_issue(issues, path, "must be a table", "type")
            continue
        item_id = item.get("id")
        if not isinstance(item_id, str) or not item_id.strip():
            _add_issue(issues, f"{path}.id", "must be a non-empty string", "required")
            continue
        normalized_id = item_id.strip()
        _check_unique(normalized_id, f"{path}.id", seen, issues)
        result.add(normalized_id)
    return result


def _required_string(
    payload: dict[str, Any],
    key: str,
    parent_path: str,
    issues: list[ConfigValidationIssue],
) -> str:
    if key not in payload:
        _add_issue(issues, f"{parent_path}.{key}", "field is required", "required")
        return ""
    return _coerce_string(payload[key], f"{parent_path}.{key}", issues, required=True)


def _optional_string(
    payload: dict[str, Any],
    key: str,
    parent_path: str,
    issues: list[ConfigValidationIssue],
    *,
    default: str,
) -> str:
    if key not in payload:
        return default
    return _coerce_string(payload[key], f"{parent_path}.{key}", issues, required=False)


def _coerce_string(
    value: Any,
    path: str,
    issues: list[ConfigValidationIssue],
    *,
    required: bool,
) -> str:
    if isinstance(value, str):
        text = value.strip()
        if text or not required:
            return text
        _add_issue(issues, path, "must be a non-empty string", "required")
        return ""
    _add_issue(issues, path, "must be a string", "type")
    return ""


def _optional_bool(
    payload: dict[str, Any],
    key: str,
    parent_path: str,
    issues: list[ConfigValidationIssue],
    *,
    default: bool,
) -> bool:
    if key not in payload:
        return default
    value = payload[key]
    if isinstance(value, bool):
        return value
    _add_issue(issues, f"{parent_path}.{key}", "must be a boolean", "type")
    return default


def _optional_int(
    payload: dict[str, Any],
    key: str,
    parent_path: str,
    issues: list[ConfigValidationIssue],
    *,
    default: int,
) -> int:
    if key not in payload:
        return default
    value = payload[key]
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    _add_issue(issues, f"{parent_path}.{key}", "must be an integer", "type")
    return default


def _required_string_list(
    payload: dict[str, Any],
    key: str,
    parent_path: str,
    issues: list[ConfigValidationIssue],
    *,
    non_empty: bool,
) -> tuple[str, ...]:
    if key not in payload:
        _add_issue(issues, f"{parent_path}.{key}", "field is required", "required")
        return ()
    return _coerce_string_list(
        payload[key],
        f"{parent_path}.{key}",
        issues,
        non_empty=non_empty,
    )


def _optional_string_list(
    payload: dict[str, Any],
    key: str,
    parent_path: str,
    issues: list[ConfigValidationIssue],
    *,
    default: tuple[str, ...],
    non_empty: bool = False,
) -> tuple[str, ...]:
    if key not in payload:
        return default
    return _coerce_string_list(
        payload[key],
        f"{parent_path}.{key}",
        issues,
        non_empty=non_empty,
    )


def _coerce_string_list(
    value: Any,
    path: str,
    issues: list[ConfigValidationIssue],
    *,
    non_empty: bool,
) -> tuple[str, ...]:
    if not isinstance(value, list):
        _add_issue(issues, path, "must be a list of strings", "type")
        return ()

    result: list[str] = []
    for index, item in enumerate(value):
        item_path = f"{path}[{index}]"
        if not isinstance(item, str):
            _add_issue(issues, item_path, "must be a string", "type")
            continue
        text = item.strip()
        if not text:
            _add_issue(issues, item_path, "must be a non-empty string", "required")
            continue
        result.append(text)

    if non_empty and not result:
        _add_issue(issues, path, "must contain at least one item", "required")
    return tuple(result)


def _validate_session_patterns(
    session_patterns: tuple[str, ...],
    path: str,
    issues: list[ConfigValidationIssue],
) -> None:
    for index, pattern in enumerate(session_patterns):
        pattern_path = f"{path}[{index}]"
        if pattern == WILDCARD:
            continue
        session_type, separator, target = pattern.partition(":")
        if not separator or session_type not in SESSION_PATTERN_TYPES or not target:
            _add_issue(
                issues,
                pattern_path,
                "must be '*', 'group:<target>', or 'private:<target>'",
                "pattern",
            )


def _validate_plugin_refs(
    plugin_refs: tuple[str, ...],
    path: str,
    plugin_ids: set[str],
    issues: list[ConfigValidationIssue],
) -> None:
    for index, plugin_id in enumerate(plugin_refs):
        if plugin_id == PLUGIN_WILDCARD:
            continue
        if plugin_id not in plugin_ids:
            _add_issue(
                issues,
                f"{path}[{index}]",
                f"unknown plugin: {plugin_id}",
                "unknown_ref",
            )


def _validate_data_relative_path(
    value: str,
    path: str,
    data_dir: Path | None,
    issues: list[ConfigValidationIssue],
) -> None:
    candidate = Path(value)
    if candidate.is_absolute():
        _add_issue(issues, path, "must be relative to data_dir", "path")
        return
    if data_dir is None:
        return

    root = data_dir.resolve()
    resolved = (root / candidate).resolve()
    try:
        resolved.relative_to(root)
    except ValueError:
        _add_issue(issues, path, "must stay within data_dir", "path")


def _check_unique(
    value: str,
    path: str,
    seen: dict[str, str],
    issues: list[ConfigValidationIssue],
) -> None:
    if value in seen:
        _add_issue(issues, path, f"duplicates {seen[value]}", "duplicate")
        return
    seen[value] = path


def _add_issue(
    issues: list[ConfigValidationIssue],
    path: str,
    message: str,
    code: str,
) -> None:
    issues.append(ConfigValidationIssue(path=path, message=message, code=code))


def _format_issues(issues: list[ConfigValidationIssue]) -> str:
    if not issues:
        return "bot service config is invalid"
    lines = ["bot service config is invalid:"]
    lines.extend(f"- {issue.path}: {issue.message} ({issue.code})" for issue in issues)
    return "\n".join(lines)
