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
        "cacheStable": payload["cache_stable"],
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


@router.get("")
async def list_prompts(bot=BotDep):
    """List all registered prompt components for dashboard selection."""
    return ok([_prompt_dict(item) for item in bot.prompt_registry.list_component_catalog()])
