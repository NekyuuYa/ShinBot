"""Agent media integration for core ingress hooks."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from shinbot.agent.media.inspection import MediaInspectionRunner
    from shinbot.agent.media.service import MediaService
    from shinbot.core.dispatch.ingress import RouteDispatchContext

logger = logging.getLogger(__name__)


class MediaIngressHook:
    """Ingest message media fingerprints after core gates pass and before routing."""

    def __init__(
        self,
        media_service: MediaService | None,
        inspection_runner: MediaInspectionRunner | None = None,
    ) -> None:
        self._media_service = media_service
        self._inspection_runner = inspection_runner

    def __call__(self, context: RouteDispatchContext) -> None:
        if self._media_service is None or context.message_log_id is None:
            return

        message_context = context.require_message_context()
        session_id = message_context.session.id
        try:
            ingested_items = self._media_service.ingest_message_media(
                session_id=session_id,
                sender_id=context.event.sender_id or "",
                platform_msg_id=(
                    context.event.message.id if context.event.message is not None else ""
                ),
                elements=context.message.elements,
                message_log_id=context.message_log_id,
                seen_at=time.time(),
            )
            if self._inspection_runner is not None and any(
                item.should_request_inspection for item in ingested_items
            ):
                self._inspection_runner.schedule_items(
                    instance_id=context.adapter.instance_id,
                    session_id=session_id,
                    items=ingested_items,
                )
        except Exception:
            logger.exception("Failed to ingest media fingerprints for session %s", session_id)


__all__ = ["MediaIngressHook"]
