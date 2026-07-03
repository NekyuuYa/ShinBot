"""Agent media integration for core ingress hooks."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from shinbot.agent.services.media.inspection import MediaInspectionRunner
    from shinbot.agent.services.media.service import MediaService
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

    async def ensure_image_descriptions(
        self,
        *,
        instance_id: str,
        session_id: str,
        messages: list[dict[str, Any]],
    ) -> None:
        """Ensure image descriptions exist for all images in the given messages.

        Waits for in-flight inspections and runs on-demand inspection for
        any images still missing a verified digest.

        Args:
            instance_id: Platform instance identifier.
            session_id: Conversation session identifier.
            messages: Message records containing images to check.
        """
        if self._media_service is None or self._inspection_runner is None:
            return

        raw_hashes = self._extract_image_hashes(messages)
        if not raw_hashes:
            return

        await self._inspection_runner.ensure_descriptions(
            instance_id=instance_id,
            session_id=session_id,
            raw_hashes=raw_hashes,
        )

    def _extract_image_hashes(self, messages: list[dict[str, Any]]) -> list[str]:
        """Extract unique image raw hashes from message records."""
        import json as _json

        from shinbot.agent.services.context.builders.message_parts import parse_message_parts
        from shinbot.agent.services.media.fingerprint import fingerprint_image_file

        hashes: list[str] = []
        seen: set[str] = set()

        for record in messages:
            try:
                parts = parse_message_parts(record)
            except Exception:
                parts = []
            for part in parts:
                if part.kind != "image" or part.image is None:
                    continue
                raw_hash = str(part.image.raw_hash or "").strip()
                if raw_hash and raw_hash not in seen:
                    seen.add(raw_hash)
                    hashes.append(raw_hash)

            content_json = str(record.get("content_json", "") or "").strip()
            if not content_json:
                continue
            try:
                payload = _json.loads(content_json)
            except (_json.JSONDecodeError, TypeError):
                continue
            if not isinstance(payload, list):
                continue
            self._collect_hashes_from_elements(payload, seen, hashes, fingerprint_image_file)

        return hashes

    def _collect_hashes_from_elements(
        self,
        elements: list[Any],
        seen: set[str],
        hashes: list[str],
        fingerprint_fn: Any,
    ) -> None:
        """Recursively collect image hashes from element trees."""
        for item in elements:
            if not isinstance(item, dict):
                continue
            attrs = item.get("attrs") if isinstance(item.get("attrs"), dict) else {}
            if item.get("type") == "img" and attrs.get("src"):
                src = str(attrs.get("src", "") or "").strip()
                if src:
                    try:
                        fingerprint = fingerprint_fn(src)
                    except Exception:
                        fingerprint = None
                    if fingerprint is not None:
                        raw_hash = fingerprint.raw_hash
                        if raw_hash and raw_hash not in seen:
                            seen.add(raw_hash)
                            hashes.append(raw_hash)
            children = item.get("children")
            if isinstance(children, list):
                self._collect_hashes_from_elements(children, seen, hashes, fingerprint_fn)


__all__ = ["MediaIngressHook"]
