"""Plugin configuration and web-facing localization helpers."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any


def plugin_module(plugin_manager: Any, plugin_id: str) -> Any | None:
    return plugin_manager._modules.get(plugin_id)


def plugin_locales(plugin_manager: Any, plugin_id: str) -> dict[str, dict[str, str]]:
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
    module = plugin_module(plugin_manager, plugin_id)
    if module is None:
        return None
    cfg_cls = getattr(module, "__plugin_config_class__", None)
    return cfg_cls if cfg_cls is not None and hasattr(cfg_cls, "model_validate") else None


def plugin_config_schema(plugin_manager: Any, plugin_id: str) -> dict[str, Any] | None:
    cfg_cls = plugin_config_class(plugin_manager, plugin_id)
    if cfg_cls is None or not hasattr(cfg_cls, "model_json_schema"):
        return None
    schema = cfg_cls.model_json_schema()
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


def plugin_config_store(boot: Any) -> dict[str, Any]:
    store = boot.config.setdefault("plugin_configs", {})
    if not isinstance(store, dict):
        store = {}
        boot.config["plugin_configs"] = store
    return store


def plugin_saved_config(boot: Any, plugin_id: str) -> dict[str, Any]:
    store = boot.config.get("plugin_configs", {})
    if not isinstance(store, dict):
        return {}
    config = store.get(plugin_id, {})
    return dict(config) if isinstance(config, dict) else {}


def plugin_state_store(boot: Any) -> dict[str, Any]:
    store = boot.config.setdefault("plugin_states", {})
    if not isinstance(store, dict):
        store = {}
        boot.config["plugin_states"] = store
    return store


def plugin_saved_enabled(boot: Any, plugin_id: str) -> bool | None:
    store = boot.config.get("plugin_states", {})
    if not isinstance(store, dict):
        return None

    state = store.get(plugin_id)
    if isinstance(state, dict):
        return normalize_plugin_enabled(state.get("enabled"))
    return normalize_plugin_enabled(state)


def set_plugin_saved_enabled(boot: Any, plugin_id: str, enabled: bool) -> None:
    store = plugin_state_store(boot)
    state = store.get(plugin_id)
    if not isinstance(state, dict):
        state = {}
    state["enabled"] = bool(enabled)
    store[plugin_id] = state


def normalize_plugin_enabled(value: Any) -> bool | None:
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
    cfg_cls = plugin_config_class(plugin_manager, plugin_id)
    expanded = unflatten_config(config)
    if cfg_cls is None:
        return expanded

    validated = cfg_cls.model_validate(expanded)
    return validated.model_dump(exclude_none=True)


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
        if value.get("type") == "object":
            _translate_schema_properties(value.get("properties"), translations, path)
        items = value.get("items")
        if isinstance(items, dict) and items.get("type") == "object":
            _translate_schema_properties(items.get("properties"), translations, path)
