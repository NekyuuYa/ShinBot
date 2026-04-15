"""Plugin configuration and web-facing localization helpers."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

def plugin_module(plugin_manager: Any, plugin_id: str) -> Any | None:
    return plugin_manager._modules.get(plugin_id)


def plugin_locales(plugin_manager: Any, plugin_id: str) -> dict[str, dict[str, str]]:
    module = plugin_module(plugin_manager, plugin_id)
    if module is None:
        return {}

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
    return cfg_cls.model_json_schema()


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
