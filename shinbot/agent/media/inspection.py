"""Asynchronous media inspection runner for repeated images/memes."""

from __future__ import annotations

import asyncio
import time
from typing import Any

from shinbot.agent.media.parsing import (
    MEDIA_INSPECTION_RESPONSE_FORMAT,
    clip_media_digest,
    parse_media_inspection_payload,
)
from shinbot.agent.media.prompt_building import (
    build_media_inspection_messages,
    build_media_reanalysis_messages,
    build_sticker_summary_messages,
)
from shinbot.agent.media.service import (
    SEMANTIC_TTL_SECONDS,
    IngestedMediaItem,
    MediaService,
)
from shinbot.agent.model_runtime import ModelCallError, ModelRuntime, ModelRuntimeCall
from shinbot.agent.prompt_manager import PromptRegistry
from shinbot.agent.prompt_manager.runtime_sync import build_runtime_component_ids
from shinbot.persistence.engine import DatabaseManager
from shinbot.persistence.records import MediaSemanticRecord
from shinbot.utils.logger import get_logger

logger = get_logger(__name__)


class MediaInspectionRunner:
    """Schedules and executes media inspection requests in the background."""

    def __init__(
        self,
        database: DatabaseManager,
        prompt_registry: PromptRegistry,
        model_runtime: ModelRuntime,
        media_service: MediaService,
    ) -> None:
        self._database = database
        self._prompt_registry = prompt_registry
        self._model_runtime = model_runtime
        self._media_service = media_service
        self._inflight: dict[str, asyncio.Task[None]] = {}

    def schedule_items(
        self,
        *,
        instance_id: str,
        session_id: str,
        items: list[IngestedMediaItem],
    ) -> None:
        for item in items:
            if not item.should_request_inspection:
                continue
            if item.raw_hash in self._inflight:
                continue

            task = asyncio.create_task(
                self._inspect_with_guard(
                    instance_id=instance_id,
                    session_id=session_id,
                    raw_hash=item.raw_hash,
                    prefer_sticker_model=item.is_custom_emoji,
                ),
                name=f"media-inspect-{item.raw_hash[:12]}",
            )
            self._inflight[item.raw_hash] = task
            task.add_done_callback(
                lambda _task, raw_hash=item.raw_hash: self._inflight.pop(raw_hash, None)
            )

    async def inspect_raw_hash(
        self,
        *,
        instance_id: str,
        session_id: str,
        raw_hash: str,
        prefer_sticker_model: bool = False,
    ) -> dict[str, Any] | None:
        semantics = self._database.media_semantics.get(raw_hash)
        if semantics is not None and bool(semantics.get("verified_by_model")):
            return semantics

        asset = self._database.media_assets.get(raw_hash)
        if asset is None:
            logger.warning("Media inspection skipped: asset %s not found", raw_hash)
            return None

        occurrence = self._database.session_media_occurrences.get(session_id, raw_hash)
        resolved = self._media_service.resolve_inspection_config(instance_id)
        selected_agent_ref = resolved.sticker_agent_ref if prefer_sticker_model else resolved.agent_ref
        selected_prompt_ref = (
            resolved.sticker_prompt_ref if prefer_sticker_model else resolved.prompt_ref
        )
        selected_llm_ref = resolved.sticker_llm_ref if prefer_sticker_model else resolved.llm_ref
        route_id, model_id, model_context_window, resolved_llm_ref = self._resolve_model_target(
            instance_id=instance_id,
            llm_ref=selected_llm_ref,
        )
        if not route_id and not model_id:
            logger.warning(
                "Media inspection skipped: no model target available for instance %s (llm=%s)",
                instance_id,
                selected_llm_ref,
            )
            return None

        try:
            if prefer_sticker_model:
                messages = build_sticker_summary_messages(
                    resolved_prompt_ref=selected_prompt_ref,
                    resolved_llm_ref=resolved_llm_ref,
                    prompt_registry=self._prompt_registry,
                    database=self._database,
                    instance_id=instance_id,
                    session_id=session_id,
                    raw_hash=raw_hash,
                    asset=asset,
                    occurrence=occurrence,
                    model_context_window=model_context_window,
                )
            else:
                messages = build_media_inspection_messages(
                    resolved_prompt_ref=selected_prompt_ref,
                    resolved_llm_ref=resolved_llm_ref,
                    prompt_registry=self._prompt_registry,
                    database=self._database,
                    instance_id=instance_id,
                    session_id=session_id,
                    raw_hash=raw_hash,
                    asset=asset,
                    occurrence=occurrence,
                    model_context_window=model_context_window,
                )
        except FileNotFoundError:
            logger.warning("Media inspection skipped: cached file missing for %s", raw_hash)
            return None
        except Exception:
            logger.exception("Failed to build media inspection prompt for %s", raw_hash)
            return None

        try:
            caller = (
                "media.sticker_summary_runner"
                if prefer_sticker_model
                else "media.inspection_runner"
            )
            purpose = "sticker_summary" if prefer_sticker_model else "media_inspection"
            result = await self._model_runtime.generate(
                ModelRuntimeCall(
                    route_id=route_id or None,
                    model_id=model_id or None,
                    caller=caller,
                    session_id=session_id,
                    instance_id=instance_id,
                    purpose=purpose,
                    messages=messages,
                    response_format=MEDIA_INSPECTION_RESPONSE_FORMAT,
                    metadata={
                        "raw_hash": raw_hash,
                        "inspection_agent_ref": selected_agent_ref,
                        "inspection_prompt_ref": selected_prompt_ref,
                        "inspection_llm_ref": resolved_llm_ref,
                        "summary_mode": "sticker" if prefer_sticker_model else "image",
                    },
                )
            )
        except ModelCallError:
            logger.exception("Media inspection model call failed for %s", raw_hash)
            return None

        payload = parse_media_inspection_payload(result.text)
        if payload is None:
            logger.warning("Media inspection returned non-JSON output for %s", raw_hash)
            return None

        now = time.time()
        digest = clip_media_digest(str(payload.get("digest") or ""))
        semantics_record = MediaSemanticRecord(
            raw_hash=raw_hash,
            kind=str(payload.get("kind") or ""),
            digest=digest,
            verified_by_model=True,
            inspection_agent_ref=selected_agent_ref,
            inspection_llm_ref=resolved_llm_ref,
            metadata={
                "inspection_prompt_ref": selected_prompt_ref,
                "confidence_band": str(payload.get("confidence_band") or ""),
                "reason": str(payload.get("reason") or ""),
                "session_id": session_id,
                "occurrence_count": int((occurrence or {}).get("occurrence_count") or 0),
                "summary_mode": "sticker" if prefer_sticker_model else "image",
                "is_custom_emoji": prefer_sticker_model,
            },
            first_seen_at=now,
            last_seen_at=now,
            expire_at=now + SEMANTIC_TTL_SECONDS,
        )
        self._database.media_semantics.upsert(semantics_record)
        return self._database.media_semantics.get(raw_hash)

    async def answer_question(
        self,
        *,
        instance_id: str,
        session_id: str,
        raw_hash: str,
        question: str,
    ) -> dict[str, Any] | None:
        asset = self._database.media_assets.get(raw_hash)
        if asset is None:
            logger.warning("Media reanalysis skipped: asset %s not found", raw_hash)
            return None

        resolved = self._media_service.resolve_inspection_config(instance_id)
        route_id, model_id, model_context_window, resolved_llm_ref = self._resolve_model_target(
            instance_id=instance_id,
            llm_ref=resolved.llm_ref,
        )
        if not route_id and not model_id:
            logger.warning(
                "Media reanalysis skipped: no model target available for instance %s (llm=%s)",
                instance_id,
                resolved.llm_ref,
            )
            return None

        try:
            messages = build_media_reanalysis_messages(
                instance_id=instance_id,
                session_id=session_id,
                raw_hash=raw_hash,
                asset=asset,
                question=question,
                model_context_window=model_context_window,
            )
        except FileNotFoundError:
            logger.warning("Media reanalysis skipped: cached file missing for %s", raw_hash)
            return None
        except Exception:
            logger.exception("Failed to build media reanalysis prompt for %s", raw_hash)
            return None

        try:
            result = await self._model_runtime.generate(
                ModelRuntimeCall(
                    route_id=route_id or None,
                    model_id=model_id or None,
                    caller="media.reanalysis_runner",
                    session_id=session_id,
                    instance_id=instance_id,
                    purpose="media_reanalysis",
                    messages=messages,
                    metadata={
                        "raw_hash": raw_hash,
                        "inspection_agent_ref": resolved.agent_ref,
                        "inspection_llm_ref": resolved_llm_ref,
                        "question": question,
                    },
                )
            )
        except ModelCallError:
            logger.exception("Media reanalysis model call failed for %s", raw_hash)
            return None

        answer = result.text.strip()
        if not answer:
            return None
        return {
            "raw_hash": raw_hash,
            "answer": answer,
            "inspection_agent_ref": resolved.agent_ref,
            "inspection_llm_ref": resolved_llm_ref,
        }

    async def shutdown(self) -> None:
        tasks = list(self._inflight.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._inflight.clear()

    async def _inspect_with_guard(
        self,
        *,
        instance_id: str,
        session_id: str,
        raw_hash: str,
        prefer_sticker_model: bool,
    ) -> None:
        try:
            await self.inspect_raw_hash(
                instance_id=instance_id,
                session_id=session_id,
                raw_hash=raw_hash,
                prefer_sticker_model=prefer_sticker_model,
            )
        except Exception:
            logger.exception("Unhandled media inspection failure for %s", raw_hash)

    def _resolve_agent(self, agent_ref: str) -> dict[str, Any] | None:
        payload = self._database.agents.get(agent_ref)
        if payload is not None:
            return payload
        return self._database.agents.get_by_agent_id(agent_ref)

    def _resolve_model_target(
        self,
        *,
        instance_id: str,
        llm_ref: str,
    ) -> tuple[str, str, int | None, str]:
        route_id, model_id, window = self._resolve_target(llm_ref)
        if route_id or model_id:
            return route_id, model_id, window, llm_ref

        return "", "", None, llm_ref

    def _resolve_target(self, target: str) -> tuple[str, str, int | None]:
        route = self._database.model_registry.get_route(target)
        if route is not None and route["enabled"]:
            members = self._database.model_registry.list_route_members(target)
            enabled_members = [member for member in members if member["enabled"]]
            enabled_members.sort(
                key=lambda item: (item["priority"], -item["weight"], item["model_id"])
            )
            for member in enabled_members:
                model = self._database.model_registry.get_model(member["model_id"])
                if model is not None and model["enabled"]:
                    return target, "", model.get("context_window")
            return target, "", None

        model = self._database.model_registry.get_model(target)
        if model is not None and model["enabled"]:
            return "", target, model.get("context_window")

        return "", "", None

    def _build_component_ids(
        self,
        agent: dict[str, Any],
        persona: dict[str, Any],
    ) -> list[str]:
        component_ids, unresolved_refs = build_runtime_component_ids(
            self._database,
            self._prompt_registry,
            persona=persona,
            agent=agent,
        )
        for prompt_ref in unresolved_refs:
            logger.warning("Skipped unresolvable media inspection prompt ref: %s", prompt_ref)
        return component_ids
