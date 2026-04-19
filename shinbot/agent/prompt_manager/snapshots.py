"""Snapshot, logging, and signature helpers for prompt assembly."""

from __future__ import annotations

import hashlib
import json

from shinbot.agent.prompt_manager.schema import (
    PromptAssemblyRequest,
    PromptAssemblyResult,
    PromptLoggerRecord,
    PromptSnapshot,
    PromptSourceType,
    PromptStageBlock,
)


def create_prompt_snapshot(
    result: PromptAssemblyResult,
    request: PromptAssemblyRequest,
) -> PromptSnapshot:
    """Create a serializable snapshot from one assembly result."""

    return PromptSnapshot(
        profile_id=result.profile_id,
        caller=result.caller,
        session_id=request.session_id,
        instance_id=request.instance_id,
        route_id=request.route_id,
        model_id=request.model_id,
        prompt_signature=result.prompt_signature,
        cache_key=result.cache_key,
        components=result.ordered_components,
        stages=result.stages,
        full_messages=result.messages,
        full_tools=result.tools,
        compatibility_used=result.compatibility_used,
        truncation=result.truncation,
        metadata=dict(result.metadata),
    )


def build_prompt_log_record(
    result: PromptAssemblyResult,
    request: PromptAssemblyRequest,
) -> PromptLoggerRecord:
    """Build a lightweight log record for one prompt assembly."""

    unknown_sources = sum(
        1
        for component in result.ordered_components
        if component.source.source_type == PromptSourceType.UNKNOWN_SOURCE
    )
    return PromptLoggerRecord(
        profile_id=result.profile_id,
        caller=result.caller,
        session_id=request.session_id,
        instance_id=request.instance_id,
        route_id=request.route_id,
        model_id=request.model_id,
        prompt_signature=result.prompt_signature,
        cache_key=result.cache_key,
        compatibility_used=result.compatibility_used,
        selected_component_count=len(result.ordered_components),
        unknown_source_count=unknown_sources,
        truncation_summary=dict(result.truncation),
        metadata=dict(result.metadata),
    )


def build_prompt_signature(stages: list[PromptStageBlock]) -> str:
    """Build a deterministic signature for assembled prompt stages."""

    payload = [
        {
            "stage": stage.stage.value,
            "components": [
                {
                    "id": component.component_id,
                    "version": component.version,
                    "text_hash": component.text_hash,
                }
                for component in stage.components
            ],
        }
        for stage in stages
    ]
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def build_prompt_cache_key(
    prompt_signature: str,
    request: PromptAssemblyRequest,
) -> str:
    """Build the cache key for one prompt assembly request."""

    payload = {
        "prompt_signature": prompt_signature,
        "route_id": request.route_id,
        "model_id": request.model_id,
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
