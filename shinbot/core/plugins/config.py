"""Plugin configuration and web-facing localization helpers."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any


def plugin_module(plugin_manager: Any, plugin_id: str) -> Any | None:
    """Resolve a plugin's Python module by its identifier.

    Args:
        plugin_manager: The plugin manager instance that owns loaded modules.
        plugin_id: The unique identifier of the plugin.

    Returns:
        The plugin's Python module, or ``None`` if not loaded.
    """
    return plugin_manager._modules.get(plugin_id)


def plugin_locales(plugin_manager: Any, plugin_id: str) -> dict[str, dict[str, str]]:
    """Collect locale translation maps for a plugin.

    Locale entries are gathered from on-disk ``locales/*.json`` files next to
    the module first; falling back to the module's ``__plugin_locales__``
    attribute when no files are found.

    Args:
        plugin_manager: The plugin manager instance.
        plugin_id: The unique identifier of the plugin.

    Returns:
        A mapping of locale codes (e.g. ``"en-US"``) to key/value translation
        dictionaries.  An empty dict is returned when the module is unknown or
        has no locale data.
    """
    module = plugin_module(plugin_manager, plugin_id)
    if module is None:
        return {}

    file_locales = _plugin_file_locales(module)
    if file_locales:
        return file_locales

    payload = getattr(module, "__plugin_locales__", {})
    if not isinstance(payload, dict):
        return {}

    result: dict[str, dict[str, str]] = {}
    for locale, entries in payload.items():
        if not isinstance(locale, str) or not isinstance(entries, dict):
            continue
        result[locale] = {
            str(key): str(value)
            for key, value in entries.items()
            if isinstance(key, str) and isinstance(value, str)
        }
    return result


def _plugin_file_locales(module: Any) -> dict[str, dict[str, str]]:
    module_file = getattr(module, "__file__", None)
    if not isinstance(module_file, str) or not module_file:
        return {}

    locales_dir = Path(module_file).resolve().parent / "locales"
    if not locales_dir.is_dir():
        return {}

    result: dict[str, dict[str, str]] = {}
    for locale_file in sorted(locales_dir.glob("*.json")):
        try:
            payload = json.loads(locale_file.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        result[locale_file.stem] = {
            str(key): str(value)
            for key, value in payload.items()
            if isinstance(key, str) and isinstance(value, str)
        }
    return result


def plugin_config_class(plugin_manager: Any, plugin_id: str) -> type[Any] | None:
    """Retrieve the Pydantic configuration class declared by a plugin.

    The plugin module must expose a ``__plugin_config_class__`` attribute whose
    value implements ``model_validate`` (i.e. is a Pydantic ``BaseModel``
    subclass).

    Args:
        plugin_manager: The plugin manager instance.
        plugin_id: The unique identifier of the plugin.

    Returns:
        The Pydantic model class, or ``None`` if the plugin has no config class.
    """
    module = plugin_module(plugin_manager, plugin_id)
    if module is None:
        return None
    cfg_cls = getattr(module, "__plugin_config_class__", None)
    return cfg_cls if cfg_cls is not None and hasattr(cfg_cls, "model_validate") else None


def plugin_config_schema(plugin_manager: Any, plugin_id: str) -> dict[str, Any] | None:
    """Return the JSON Schema for a plugin's configuration.

    ``$ref`` and ``$defs`` definitions are inlined so that frontend consumers
    receive a self-contained schema.

    Args:
        plugin_manager: The plugin manager instance.
        plugin_id: The unique identifier of the plugin.

    Returns:
        An inlined JSON Schema dict, or ``None`` when the plugin has no
        config class.
    """
    cfg_cls = plugin_config_class(plugin_manager, plugin_id)
    if cfg_cls is None or not hasattr(cfg_cls, "model_json_schema"):
        return None
    schema = dict(cfg_cls.model_json_schema())
    return _resolve_refs(schema)


def _resolve_refs(schema: dict[str, Any]) -> dict[str, Any]:
    """Inline all $ref definitions so the frontend never sees $ref or $defs."""
    defs = schema.get("$defs", {})
    if not defs:
        return schema

    import copy

    resolved = copy.deepcopy(schema)

    def _inline(node: Any) -> Any:
        if not isinstance(node, dict):
            return node
        if "$ref" in node:
            ref_path = node["$ref"]
            if ref_path.startswith("#/$defs/"):
                def_name = ref_path[len("#/$defs/"):]
                if def_name in defs:
                    inlined = _inline(copy.deepcopy(defs[def_name]))
                    # Merge any sibling keys from the original ref node (e.g. title overrides)
                    extra = {k: v for k, v in node.items() if k != "$ref"}
                    if extra:
                        inlined = {**inlined, **extra}
                    return inlined
        return {k: _inline(v) for k, v in node.items()}

    result = _inline(resolved)
    result.pop("$defs", None)
    return result


def plugin_config_entry(
    config: dict[str, Any],
    plugin_id: str,
    *,
    create: bool = False,
) -> dict[str, Any] | None:
    """Look up (and optionally create) a plugin's entry in the main config dict.

    Plugins are stored as a list under the ``"plugins"`` key.  Each entry is a
    dict containing at least ``"id"``.

    Args:
        config: The top-level bot configuration dictionary.
        plugin_id: The unique identifier of the plugin.
        create: When ``True`` and no entry exists, a new one is appended with
            default ``enabled=True`` and an empty ``config`` block.

    Returns:
        The existing or newly created config entry, or ``None`` when not found
        and *create* is ``False``.
    """
    plugins = config.setdefault("plugins", []) if create else config.get("plugins", [])
    if not isinstance(plugins, list):
        if not create:
            return None
        plugins = []
        config["plugins"] = plugins

    for plugin_item in plugins:
        if isinstance(plugin_item, dict) and plugin_item.get("id") == plugin_id:
            return plugin_item

    if not create:
        return None

    created: dict[str, Any] = {"id": plugin_id, "enabled": True, "config": {}}
    plugins.append(created)
    return created


def plugin_config_block(
    config: dict[str, Any],
    plugin_id: str,
) -> dict[str, Any]:
    """Return a plugin's config block from the main configuration.

    This is a convenience wrapper around :func:`plugin_config_entry` that
    extracts the ``"config"`` sub-dict.

    Args:
        config: The top-level bot configuration dictionary.
        plugin_id: The unique identifier of the plugin.

    Returns:
        A **copy** of the plugin's config block, or an empty dict when the
        plugin entry or its config sub-dict is missing.
    """
    item = plugin_config_entry(config, plugin_id)
    if item is not None:
        block = item.get("config", {})
        if isinstance(block, dict):
            return dict(block)
    return {}


def plugin_saved_config(
    boot: Any,
    plugin_id: str,
) -> dict[str, Any]:
    """Retrieve the persisted config block for a plugin from the boot context.

    Args:
        boot: The ``BootController`` (or equivalent) holding ``boot.config``.
        plugin_id: The unique identifier of the plugin.

    Returns:
        A copy of the plugin's config block, or an empty dict.
    """
    return plugin_config_block(boot.config, plugin_id)


def plugin_saved_enabled(
    boot: Any,
    plugin_id: str,
) -> bool | None:
    """Check whether a plugin is enabled in the persisted configuration.

    Args:
        boot: The ``BootController`` (or equivalent) holding ``boot.config``.
        plugin_id: The unique identifier of the plugin.

    Returns:
        ``True`` if enabled, ``False`` if explicitly disabled, or ``None`` when
        the plugin entry has no ``enabled`` field.
    """
    item = plugin_config_entry(boot.config, plugin_id)
    if item is not None and "enabled" in item:
        return normalize_plugin_enabled(item.get("enabled"))
    return None


def set_plugin_saved_enabled(boot: Any, plugin_id: str, enabled: bool) -> None:
    """Persist the enabled/disabled state for a plugin.

    If the plugin entry does not yet exist it will be created.

    Args:
        boot: The ``BootController`` (or equivalent) holding ``boot.config``.
        plugin_id: The unique identifier of the plugin.
        enabled: ``True`` to enable, ``False`` to disable.
    """
    item = plugin_config_entry(boot.config, plugin_id, create=True)
    assert item is not None
    item["enabled"] = bool(enabled)


def normalize_plugin_enabled(value: Any) -> bool | None:
    """Coerce a raw enabled value to a boolean or ``None``.

    Accepts booleans, numeric values (0 → ``False``), and common string
    representations (``"true"``, ``"yes"``, ``"on"``, ``"enabled"``, etc.).

    Args:
        value: The raw value from configuration.

    Returns:
        ``True`` / ``False`` for recognised truthy/falsy values, or ``None``
        when the value cannot be interpreted.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0

    normalized = str(value or "").strip().lower()
    if not normalized:
        return None
    if normalized in {"1", "true", "yes", "on", "enabled"}:
        return True
    if normalized in {"0", "false", "no", "off", "disabled"}:
        return False
    return None


def request_locales(header: str) -> list[str]:
    """Parse an HTTP ``Accept-Language`` header into an ordered locale list.

    ``zh-CN`` and ``en-US`` are always appended as fallbacks so that at least
    one common locale is present.

    Args:
        header: The raw ``Accept-Language`` header value.

    Returns:
        A list of locale codes ordered by client preference with fallbacks at
        the end.
    """
    locales: list[str] = []
    for chunk in header.split(","):
        code = chunk.split(";")[0].strip()
        if code:
            locales.append(code)
    locales.extend(["zh-CN", "en-US"])
    return locales


def resolve_translations(
    locales: dict[str, dict[str, str]], requested: list[str]
) -> dict[str, str]:
    """Select the best matching translation map from available locales.

    Matching proceeds in *requested* order.  An exact match is preferred; if
    none is found, a language-only prefix match (e.g. ``"en"`` for ``"en-GB"``)
    is attempted.

    Args:
        locales: Available locale → translation mappings.
        requested: Ordered list of preferred locale codes (highest priority
            first).

    Returns:
        The matched translation dict, or an empty dict when no locale matches.
    """
    for locale in requested:
        if locale in locales:
            return locales[locale]
        base = locale.split("-")[0].lower()
        for candidate, entries in locales.items():
            if candidate.split("-")[0].lower() == base:
                return entries
    return {}


def translate_plugin_schema(
    schema: dict[str, Any] | None,
    translations: dict[str, str],
) -> dict[str, Any] | None:
    """Apply translations to a JSON Schema used by the plugin config UI.

    Supported translation keys: ``config.title``, ``config.description``, and
    ``config.fields.<path>.label``, ``config.fields.<path>.description``,
    ``config.fields.<path>.options.<value>`` for nested properties.

    Args:
        schema: The original JSON Schema dict, or ``None``.
        translations: A flat mapping of translation keys to translated strings.

    Returns:
        A deep copy of *schema* with translated strings injected, or ``None``
        when *schema* is ``None``.
    """
    if schema is None:
        return None

    translated = deepcopy(schema)
    if "config.title" in translations:
        translated["title"] = translations["config.title"]
    if "config.description" in translations:
        translated["description"] = translations["config.description"]
    _translate_schema_properties(translated.get("properties"), translations)
    return translated


def unflatten_config(raw: dict[str, Any]) -> dict[str, Any]:
    """Expand dot-separated flat keys into a nested dictionary.

    For example, ``{"a.b.c": 1}`` becomes ``{"a": {"b": {"c": 1}}}``.

    Args:
        raw: A flat configuration mapping with dot-separated keys.

    Returns:
        A new nested dictionary.
    """
    nested: dict[str, Any] = {}
    for key, value in raw.items():
        if not isinstance(key, str) or not key:
            continue

        cursor = nested
        parts = [part for part in key.split(".") if part]
        if not parts:
            continue

        for part in parts[:-1]:
            child = cursor.get(part)
            if not isinstance(child, dict):
                child = {}
                cursor[part] = child
            cursor = child

        cursor[parts[-1]] = value

    return nested


def normalize_plugin_config(
    plugin_manager: Any,
    plugin_id: str,
    config: dict[str, Any],
) -> dict[str, Any]:
    """Expand and validate a plugin's raw config through its Pydantic model.

    Flat dot-separated keys are first expanded via :func:`unflatten_config`.
    If the plugin declares a config class, the expanded dict is validated and
    ``None``-valued fields are stripped.  When no config class exists the
    expanded dict is returned as-is.

    Args:
        plugin_manager: The plugin manager instance.
        plugin_id: The unique identifier of the plugin.
        config: Raw plugin configuration with flat or nested keys.

    Returns:
        The validated and cleaned configuration dictionary.
    """
    cfg_cls = plugin_config_class(plugin_manager, plugin_id)
    expanded = unflatten_config(config)
    if cfg_cls is None:
        return expanded

    validated = cfg_cls.model_validate(expanded)
    return dict(validated.model_dump(exclude_none=True))


def _translate_schema_properties(
    properties: dict[str, Any] | None,
    translations: dict[str, str],
    parent: str = "",
) -> None:
    if not isinstance(properties, dict):
        return
    for key, value in properties.items():
        if not isinstance(value, dict):
            continue
        path = f"{parent}.{key}" if parent else key
        label_key = f"config.fields.{path}.label"
        description_key = f"config.fields.{path}.description"
        if label_key in translations:
            value["title"] = translations[label_key]
        if description_key in translations:
            value["description"] = translations[description_key]
        if isinstance(value.get("enum"), list):
            value["enum_titles"] = [
                translations.get(
                    f"config.fields.{path}.options.{enum_value}",
                    (
                        value.get("enum_titles", [])[index]
                        if isinstance(value.get("enum_titles"), list)
                        and index < len(value.get("enum_titles", []))
                        else str(enum_value)
                    ),
                )
                for index, enum_value in enumerate(value["enum"])
            ]
        if value.get("type") == "object":
            _translate_schema_properties(value.get("properties"), translations, path)
        items = value.get("items")
        if isinstance(items, dict) and items.get("type") == "object":
            _translate_schema_properties(items.get("properties"), translations, path)
