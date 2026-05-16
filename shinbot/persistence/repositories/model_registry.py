"""File-backed model provider/model/route registry."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING, Any

from shinbot.persistence.records import (
    ModelDefinitionRecord,
    ModelProviderRecord,
    ModelRouteMemberRecord,
    ModelRouteRecord,
    utc_now_iso,
)

if TYPE_CHECKING:
    from shinbot.persistence.engine import DatabaseManager


MODEL_REGISTRY_FILE_VERSION = 1
MODEL_REGISTRY_FILENAME = "models.json"


class ModelRegistryError(ValueError):
    """Raised when the file-backed model registry cannot be read or written."""


class ModelRegistryRepository:
    """Provider/model/route registry stored as a single editable JSON file."""

    def __init__(self, source: DatabaseManager | Path | str) -> None:
        self.path = _resolve_registry_path(source)

    @classmethod
    def from_data_dir(cls, data_dir: Path | str) -> ModelRegistryRepository:
        return cls(Path(data_dir) / MODEL_REGISTRY_FILENAME)

    def ensure_file(self) -> Path:
        if not self.path.exists():
            self._write_payload(_empty_payload())
        return self.path

    def upsert_provider(self, record: ModelProviderRecord) -> None:
        payload = self._read_payload()
        provider = _provider_from_record(record)
        existing = _find_by_id(payload["providers"], provider["id"])
        if existing is not None:
            provider["created_at"] = existing.get("created_at") or provider["created_at"]
            existing.clear()
            existing.update(provider)
        else:
            payload["providers"].append(provider)
        self._write_payload(payload)

    def list_providers(self) -> list[dict[str, Any]]:
        payload = self._read_payload()
        providers = [_normalize_provider(item) for item in payload["providers"]]
        providers.sort(key=lambda item: item["id"])
        return providers

    def get_provider(self, provider_id: str) -> dict[str, Any] | None:
        payload = self._read_payload()
        provider = _find_by_id(payload["providers"], provider_id)
        return _normalize_provider(provider) if provider is not None else None

    def delete_provider(self, provider_id: str) -> int:
        payload = self._read_payload()
        providers = payload["providers"]
        provider = _find_by_id(providers, provider_id)
        if provider is None:
            return 0

        removed_model_ids = {
            str(model.get("id"))
            for model in payload["models"]
            if str(model.get("provider_id") or "") == provider_id
        }
        payload["providers"] = [item for item in providers if str(item.get("id")) != provider_id]
        payload["models"] = [
            item
            for item in payload["models"]
            if str(item.get("provider_id") or "") != provider_id
        ]
        _remove_route_members(payload, removed_model_ids)
        self._write_payload(payload)
        return 1

    def rename_provider(self, provider_id: str, new_provider_id: str) -> None:
        if provider_id == new_provider_id:
            return
        payload = self._read_payload()
        provider = _find_by_id(payload["providers"], provider_id)
        if provider is None:
            return
        if _find_by_id(payload["providers"], new_provider_id) is not None:
            raise ModelRegistryError(f"Provider {new_provider_id!r} already exists")

        provider["id"] = new_provider_id
        provider["updated_at"] = utc_now_iso()
        for model in payload["models"]:
            if str(model.get("provider_id") or "") == provider_id:
                model["provider_id"] = new_provider_id
                model["updated_at"] = utc_now_iso()
        self._write_payload(payload)

    def upsert_model(self, record: ModelDefinitionRecord) -> None:
        payload = self._read_payload()
        model = _model_from_record(record)
        if _find_by_id(payload["providers"], model["provider_id"]) is None:
            raise ModelRegistryError(f"Provider {model['provider_id']!r} not found")

        existing = _find_by_id(payload["models"], model["id"])
        if existing is not None:
            model["created_at"] = existing.get("created_at") or model["created_at"]
            existing.clear()
            existing.update(model)
        else:
            payload["models"].append(model)
        self._write_payload(payload)

    def list_models(self, *, provider_id: str | None = None) -> list[dict[str, Any]]:
        payload = self._read_payload()
        provider_ids = {str(item.get("id")) for item in payload["providers"]}
        models: list[dict[str, Any]] = []
        for item in payload["models"]:
            model = _normalize_model(item)
            if model["provider_id"] not in provider_ids:
                continue
            if provider_id and model["provider_id"] != provider_id:
                continue
            models.append(model)
        models.sort(key=lambda item: (item["provider_id"], item["id"]))
        return models

    def get_model(self, model_id: str) -> dict[str, Any] | None:
        payload = self._read_payload()
        model = _find_by_id(payload["models"], model_id)
        if model is None:
            return None
        normalized = _normalize_model(model)
        if _find_by_id(payload["providers"], normalized["provider_id"]) is None:
            return None
        return normalized

    def delete_model(self, model_id: str) -> int:
        payload = self._read_payload()
        model = _find_by_id(payload["models"], model_id)
        if model is None:
            return 0

        payload["models"] = [item for item in payload["models"] if str(item.get("id")) != model_id]
        _remove_route_members(payload, {model_id})
        self._write_payload(payload)
        return 1

    def upsert_route(
        self,
        record: ModelRouteRecord,
        *,
        members: list[ModelRouteMemberRecord] | None = None,
    ) -> None:
        payload = self._read_payload()
        route = _route_from_record(record)
        if members is not None:
            known_model_ids = {str(item.get("id")) for item in payload["models"]}
            missing_ids = [
                member.model_id for member in members if member.model_id not in known_model_ids
            ]
            if missing_ids:
                raise ModelRegistryError(f"Model {missing_ids[0]!r} not found")
            route["members"] = [_route_member_from_record(member) for member in members]

        existing = _find_by_id(payload["routes"], route["id"])
        if existing is not None:
            route["created_at"] = existing.get("created_at") or route["created_at"]
            if members is None:
                route["members"] = _route_members_from_file(existing)
            existing.clear()
            existing.update(route)
        else:
            route.setdefault("members", [])
            payload["routes"].append(route)
        self._write_payload(payload)

    def list_routes(self) -> list[dict[str, Any]]:
        payload = self._read_payload()
        routes = [_normalize_route(item) for item in payload["routes"]]
        routes.sort(key=lambda item: item["id"])
        return routes

    def get_route(self, route_id: str) -> dict[str, Any] | None:
        payload = self._read_payload()
        route = _find_by_id(payload["routes"], route_id)
        return _normalize_route(route) if route is not None else None

    def delete_route(self, route_id: str) -> int:
        payload = self._read_payload()
        route = _find_by_id(payload["routes"], route_id)
        if route is None:
            return 0
        payload["routes"] = [item for item in payload["routes"] if str(item.get("id")) != route_id]
        self._write_payload(payload)
        return 1

    def rename_route(self, route_id: str, new_route_id: str) -> None:
        if route_id == new_route_id:
            return
        payload = self._read_payload()
        route = _find_by_id(payload["routes"], route_id)
        if route is None:
            return
        if _find_by_id(payload["routes"], new_route_id) is not None:
            raise ModelRegistryError(f"Route {new_route_id!r} already exists")
        route["id"] = new_route_id
        route["updated_at"] = utc_now_iso()
        self._write_payload(payload)

    def list_route_members(self, route_id: str) -> list[dict[str, Any]]:
        payload = self._read_payload()
        route = _find_by_id(payload["routes"], route_id)
        if route is None:
            return []
        members = [
            _normalize_route_member(member, index=index)
            for index, member in enumerate(_route_members_from_file(route))
        ]
        members.sort(key=lambda item: (item["priority"], item["_index"]))
        for member in members:
            member.pop("_index", None)
        return members

    def _read_payload(self) -> dict[str, Any]:
        if not self.path.exists():
            return _empty_payload()
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ModelRegistryError(f"Invalid model registry JSON: {exc}") from exc
        if not isinstance(raw, dict):
            raise ModelRegistryError("Model registry root must be an object")
        return _normalize_payload(raw)

    def _write_payload(self, payload: dict[str, Any]) -> None:
        normalized = _normalize_payload(payload)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.path.with_name(f".{self.path.name}.tmp")
        temp_path.write_text(
            json.dumps(normalized, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        temp_path.replace(self.path)


def _resolve_registry_path(source: DatabaseManager | Path | str) -> Path:
    if isinstance(source, (Path, str)):
        return Path(source)
    config = source.config
    data_dir = Path(getattr(config, "data_dir", config.sqlite_path.parent.parent))
    return data_dir / MODEL_REGISTRY_FILENAME


def _empty_payload() -> dict[str, Any]:
    return {
        "version": MODEL_REGISTRY_FILE_VERSION,
        "providers": [],
        "models": [],
        "routes": [],
    }


def _normalize_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "version": MODEL_REGISTRY_FILE_VERSION,
        "providers": [_normalize_provider(item) for item in _list_of_maps(payload.get("providers"))],
        "models": [_normalize_model(item) for item in _list_of_maps(payload.get("models"))],
        "routes": [_normalize_route(item) for item in _list_of_maps(payload.get("routes"))],
    }


def _list_of_maps(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _find_by_id(items: list[dict[str, Any]], item_id: str) -> dict[str, Any] | None:
    for item in items:
        if str(item.get("id")) == item_id:
            return item
    return None


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _string(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value)


def _bool(value: Any, default: bool = True) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip().lower() not in {"0", "false", "no", "off"}
    return bool(value)


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_provider(payload: Mapping[str, Any]) -> dict[str, Any]:
    provider_id = _string(payload.get("id")).strip()
    now = utc_now_iso()
    return {
        "id": provider_id,
        "type": _string(payload.get("type")).strip(),
        "display_name": _string(payload.get("display_name"), provider_id).strip(),
        "capability_type": _string(payload.get("capability_type"), "completion").strip()
        or "completion",
        "base_url": _string(payload.get("base_url")).strip(),
        "auth": _mapping(payload.get("auth")),
        "default_params": _mapping(payload.get("default_params")),
        "enabled": _bool(payload.get("enabled"), True),
        "created_at": _string(payload.get("created_at"), now),
        "updated_at": _string(payload.get("updated_at"), now),
    }


def _normalize_model(payload: Mapping[str, Any]) -> dict[str, Any]:
    model_id = _string(payload.get("id")).strip()
    now = utc_now_iso()
    capabilities = payload.get("capabilities")
    return {
        "id": model_id,
        "provider_id": _string(payload.get("provider_id")).strip(),
        "litellm_model": _string(payload.get("litellm_model")).strip(),
        "display_name": _string(payload.get("display_name"), model_id).strip(),
        "capabilities": [
            str(item)
            for item in capabilities
            if item is not None
        ]
        if isinstance(capabilities, list)
        else [],
        "context_window": _int_or_none(payload.get("context_window")),
        "default_params": _mapping(payload.get("default_params")),
        "cost_metadata": _mapping(payload.get("cost_metadata")),
        "enabled": _bool(payload.get("enabled"), True),
        "created_at": _string(payload.get("created_at"), now),
        "updated_at": _string(payload.get("updated_at"), now),
    }


def _normalize_route(payload: Mapping[str, Any]) -> dict[str, Any]:
    route_id = _string(payload.get("id")).strip()
    now = utc_now_iso()
    return {
        "id": route_id,
        "purpose": _string(payload.get("purpose")),
        "strategy": _string(payload.get("strategy"), "priority").strip() or "priority",
        "enabled": _bool(payload.get("enabled"), True),
        "sticky_sessions": _bool(payload.get("sticky_sessions"), False),
        "metadata": _mapping(payload.get("metadata")),
        "members": _route_members_from_file(payload),
        "created_at": _string(payload.get("created_at"), now),
        "updated_at": _string(payload.get("updated_at"), now),
    }


def _normalize_route_member(payload: Mapping[str, Any], *, index: int | None = None) -> dict[str, Any]:
    member = {
        "model_id": _string(payload.get("model_id")).strip(),
        "priority": _int_or_none(payload.get("priority")) or 0,
        "weight": _float_or_none(payload.get("weight")) or 1.0,
        "conditions": _mapping(payload.get("conditions")),
        "timeout_override": _float_or_none(payload.get("timeout_override")),
        "enabled": _bool(payload.get("enabled"), True),
    }
    if index is not None:
        member["_index"] = index
    return member


def _route_members_from_file(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        _normalize_route_member(member)
        for member in _list_of_maps(payload.get("members"))
    ]


def _provider_from_record(record: ModelProviderRecord) -> dict[str, Any]:
    return _normalize_provider(asdict(record))


def _model_from_record(record: ModelDefinitionRecord) -> dict[str, Any]:
    return _normalize_model(asdict(record))


def _route_from_record(record: ModelRouteRecord) -> dict[str, Any]:
    return _normalize_route(asdict(record))


def _route_member_from_record(record: ModelRouteMemberRecord) -> dict[str, Any]:
    payload = asdict(record)
    return _normalize_route_member(payload)


def _remove_route_members(payload: dict[str, Any], model_ids: set[str]) -> None:
    if not model_ids:
        return
    for route in payload["routes"]:
        route["members"] = [
            member
            for member in _route_members_from_file(route)
            if member["model_id"] not in model_ids
        ]


__all__ = [
    "MODEL_REGISTRY_FILENAME",
    "MODEL_REGISTRY_FILE_VERSION",
    "ModelRegistryError",
    "ModelRegistryRepository",
]
