"""Tool management router: /api/v1/tools"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

from shinbot.api.deps import AuthRequired, BotDep
from shinbot.api.models import Envelope, ok

router = APIRouter(
    prefix="/tools",
    tags=["tools"],
    dependencies=AuthRequired,
)


class ToolData(BaseModel):
    id: str
    name: str
    displayName: str
    description: str
    inputSchema: dict[str, Any]
    outputSchema: dict[str, Any] | None = None
    ownerType: str
    ownerId: str
    ownerModule: str
    permission: str
    enabled: bool
    visibility: str
    timeoutSeconds: float
    riskLevel: str
    tags: list[str]
    metadata: dict[str, Any]


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


@router.get("", response_model=Envelope[list[ToolData]])
async def list_tools(bot=BotDep):
    """List all registered tools for dashboard management."""
    tool_registry = getattr(bot, "tool_registry", None)
    if tool_registry is None:
        agent_runtime = getattr(bot, "agent_runtime", None)
        tool_registry = getattr(agent_runtime, "tool_registry", None)
    if tool_registry is None:
        return ok([])
    return ok([_tool_dict(item) for item in tool_registry.list_tools()])
