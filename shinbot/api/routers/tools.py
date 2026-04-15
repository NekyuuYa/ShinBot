"""Tool management router: /api/v1/tools"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from shinbot.api.deps import AuthRequired, BotDep
from shinbot.api.models import ok

router = APIRouter(
    prefix="/tools",
    tags=["tools"],
    dependencies=AuthRequired,
)


def _tool_dict(definition: Any) -> dict[str, Any]:
    return {
        "id": definition.id,
        "name": definition.name,
        "displayName": definition.display_name or definition.name,
        "description": definition.description,
        "inputSchema": definition.input_schema,
        "outputSchema": definition.output_schema,
        "ownerType": definition.owner_type.value,
        "ownerId": definition.owner_id,
        "ownerModule": definition.owner_module,
        "permission": definition.permission,
        "enabled": definition.enabled,
        "visibility": definition.visibility.value,
        "timeoutSeconds": definition.timeout_seconds,
        "riskLevel": definition.risk_level.value,
        "tags": definition.tags,
        "metadata": definition.metadata,
    }


@router.get("")
async def list_tools(bot=BotDep):
    """List all registered tools for dashboard management."""
    return ok([_tool_dict(item) for item in bot.tool_registry.list_tools()])
