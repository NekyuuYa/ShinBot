"""Prompt registry catalog router: /api/v1/prompts"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from shinbot.api.deps import AuthRequired, BotDep
from shinbot.api.models import ok

router = APIRouter(
    prefix="/prompts",
    tags=["prompts"],
    dependencies=AuthRequired,
)


def _prompt_dict(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": payload["id"],
        "displayName": payload["display_name"],
        "description": payload["description"],
        "stage": payload["stage"],
        "type": payload["type"],
        "version": payload["version"],
        "priority": payload["priority"],
        "enabled": payload["enabled"],
        "resolverRef": payload["resolver_ref"],
        "templateVars": payload["template_vars"],
        "bundleRefs": payload["bundle_refs"],
        "tags": payload["tags"],
        "sourceType": payload["source_type"],
        "sourceId": payload["source_id"],
        "ownerPluginId": payload["owner_plugin_id"],
        "ownerModule": payload["owner_module"],
        "modulePath": payload["module_path"],
        "metadata": payload["metadata"],
    }


def _prompt_definition_dict(payload: dict[str, Any]) -> dict[str, Any]:
    metadata = dict(payload.get("metadata") or {})
    return {
        "id": payload["prompt_id"],
        "displayName": str(
            metadata.get("display_name") or payload.get("name") or payload["prompt_id"]
        ),
        "description": str(metadata.get("description") or payload.get("description") or ""),
        "stage": payload["stage"],
        "type": payload["type"],
        "version": payload["version"],
        "priority": payload["priority"],
        "enabled": payload["enabled"],
        "resolverRef": payload["resolver_ref"],
        "templateVars": payload["template_vars"],
        "bundleRefs": payload["bundle_refs"],
        "tags": payload["tags"],
        "sourceType": payload["source_type"],
        "sourceId": payload["source_id"],
        "ownerPluginId": payload["owner_plugin_id"],
        "ownerModule": payload["owner_module"],
        "modulePath": payload["module_path"],
        "metadata": metadata,
    }


@router.get("")
async def list_prompts(bot=BotDep):
    """List all registered prompt components for dashboard selection."""
    prompt_registry = getattr(bot, "prompt_registry", None)
    items_by_id: dict[str, dict[str, Any]] = {}
    if prompt_registry is not None:
        items_by_id.update(
            {
                item["id"]: _prompt_dict(item)
                for item in prompt_registry.list_component_catalog()
            }
        )

    if bot.database is not None:
        for payload in bot.database.prompt_definitions.list():
            item = _prompt_definition_dict(payload)
            items_by_id.setdefault(item["id"], item)

    items = sorted(
        items_by_id.values(),
        key=lambda item: (
            str(item["stage"]),
            int(item["priority"]),
            str(item["id"]),
        ),
    )
    return ok(items)
