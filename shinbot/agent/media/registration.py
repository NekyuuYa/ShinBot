"""Media module runtime registration helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from shinbot.agent.media.tools import register_media_tools

if TYPE_CHECKING:
    from shinbot.agent.media.inspection import MediaInspectionRunner
    from shinbot.agent.media.service import MediaService
    from shinbot.agent.tools.registry import ToolRegistry


def register_media_runtime(
    registry: ToolRegistry,
    *,
    media_service: MediaService | None,
    inspection_runner: MediaInspectionRunner | None,
) -> None:
    """Register all media runtime integrations for the current process."""

    if media_service is None or inspection_runner is None:
        return
    register_media_tools(
        registry,
        media_service,
        inspection_runner,
    )
