"""Model route/model resolution helpers for workflow runners."""

from __future__ import annotations

from typing import TYPE_CHECKING

from shinbot.agent.runtime.instance_config import parse_tagged_llm_ref

if TYPE_CHECKING:
    from shinbot.persistence.engine import DatabaseManager


def resolve_model_target(
    database: DatabaseManager,
    target: str,
) -> tuple[str, str, int | None]:
    """Resolve a route/model reference into runtime routing coordinates."""

    tagged = parse_tagged_llm_ref(target)
    if tagged is not None:
        if tagged.route_id:
            return _resolve_route_target(database, tagged.route_id)
        if tagged.model_id:
            return _resolve_model_id_target(database, tagged.model_id)
        return "", "", None

    route_id, model_id, window = _resolve_route_target(database, target)
    if route_id or model_id:
        return route_id, model_id, window
    return _resolve_model_id_target(database, target)


def _resolve_route_target(
    database: DatabaseManager,
    target: str,
) -> tuple[str, str, int | None]:
    route = database.model_registry.get_route(target)
    if route is not None and route["enabled"]:
        members = database.model_registry.list_route_members(target)
        enabled_members = [member for member in members if member["enabled"]]
        enabled_members.sort(key=lambda item: (item["priority"], -item["weight"], item["model_id"]))
        for member in enabled_members:
            model = database.model_registry.get_model(member["model_id"])
            if model is not None and model["enabled"]:
                return target, "", model.get("context_window")
        return target, "", None

    return "", "", None


def _resolve_model_id_target(
    database: DatabaseManager,
    target: str,
) -> tuple[str, str, int | None]:
    model = database.model_registry.get_model(target)
    if model is not None and model["enabled"]:
        return "", target, model.get("context_window")

    return "", "", None
