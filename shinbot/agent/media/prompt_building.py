"""Prompt/message construction helpers for media inspection workflows."""

from __future__ import annotations

import base64
from typing import Any

from shinbot.agent.media.config import (
    BUILTIN_MEDIA_INSPECTION_PROMPT,
    BUILTIN_MEDIA_INSPECTION_PROMPT_ID,
    BUILTIN_STICKER_SUMMARY_PROMPT,
    BUILTIN_STICKER_SUMMARY_PROMPT_ID,
)
from shinbot.agent.prompt_manager import PromptAssemblyRequest
from shinbot.agent.prompt_manager.rendering import infer_component_source, render_component_text
from shinbot.agent.prompt_manager.runtime_sync import sync_prompt_definition_component

MEDIA_REANALYSIS_SYSTEM_PROMPT = """
You are ShinBot's media reanalysis agent.

Answer the user's question about the supplied image as a normal image understanding task.
Describe only what is visibly supported by the image.
If identity, source character, or text content is uncertain, say so explicitly.
Prefer concise Chinese answers that are useful inside a chat workflow.
""".strip()


def build_media_inspection_messages(
    *,
    resolved_prompt_ref: str,
    resolved_llm_ref: str,
    prompt_registry: Any,
    database: Any,
    instance_id: str,
    session_id: str,
    raw_hash: str,
    asset: dict[str, Any],
    occurrence: dict[str, Any] | None,
    model_context_window: int | None,
) -> list[dict[str, Any]]:
    """Build inspection messages, falling back to builtin prompt when needed."""

    instruction_text = build_media_inspection_instruction_text(
        session_id=session_id,
        raw_hash=raw_hash,
        asset=asset,
        occurrence=occurrence,
    )
    return _build_media_prompt_messages(
        builtin_prompt=BUILTIN_MEDIA_INSPECTION_PROMPT,
        builtin_prompt_id=BUILTIN_MEDIA_INSPECTION_PROMPT_ID,
        caller="media.inspection_runner",
        trigger="media_inspection",
        instruction_text=instruction_text,
        resolved_prompt_ref=resolved_prompt_ref,
        resolved_llm_ref=resolved_llm_ref,
        prompt_registry=prompt_registry,
        database=database,
        instance_id=instance_id,
        session_id=session_id,
        raw_hash=raw_hash,
        asset=asset,
        model_context_window=model_context_window,
    )


def build_sticker_summary_messages(
    *,
    resolved_prompt_ref: str,
    resolved_llm_ref: str,
    prompt_registry: Any,
    database: Any,
    instance_id: str,
    session_id: str,
    raw_hash: str,
    asset: dict[str, Any],
    occurrence: dict[str, Any] | None,
    model_context_window: int | None,
) -> list[dict[str, Any]]:
    """Build custom-sticker summary messages with a dedicated builtin prompt fallback."""

    instruction_text = build_sticker_summary_instruction_text(
        session_id=session_id,
        raw_hash=raw_hash,
        asset=asset,
        occurrence=occurrence,
    )
    return _build_media_prompt_messages(
        builtin_prompt=BUILTIN_STICKER_SUMMARY_PROMPT,
        builtin_prompt_id=BUILTIN_STICKER_SUMMARY_PROMPT_ID,
        caller="media.sticker_summary_runner",
        trigger="sticker_summary",
        instruction_text=instruction_text,
        resolved_prompt_ref=resolved_prompt_ref,
        resolved_llm_ref=resolved_llm_ref,
        prompt_registry=prompt_registry,
        database=database,
        instance_id=instance_id,
        session_id=session_id,
        raw_hash=raw_hash,
        asset=asset,
        model_context_window=model_context_window,
    )


def _build_media_prompt_messages(
    *,
    builtin_prompt: str,
    builtin_prompt_id: str,
    caller: str,
    trigger: str,
    instruction_text: str,
    resolved_prompt_ref: str,
    resolved_llm_ref: str,
    prompt_registry: Any,
    database: Any,
    instance_id: str,
    session_id: str,
    raw_hash: str,
    asset: dict[str, Any],
    model_context_window: int | None,
) -> list[dict[str, Any]]:
    component_ids = _resolve_prompt_component_ids(
        prompt_registry=prompt_registry,
        database=database,
        prompt_refs=[resolved_prompt_ref],
        fallback_component_id=builtin_prompt_id,
    )
    request = PromptAssemblyRequest(
        caller=caller,
        identity_enabled=False,
        session_id="",
        instance_id=instance_id,
        model_context_window=model_context_window,
        component_overrides=component_ids,
        template_inputs={
            "session_id": session_id,
            "instance_id": instance_id,
            "platform": "",
            "message_text": instruction_text,
            "user_id": "",
        },
        metadata={
            "trigger": trigger,
            "inspection_prompt_ref": resolved_prompt_ref,
            "inspection_llm_ref": resolved_llm_ref,
            "raw_hash": raw_hash,
        },
    )
    prompt_text = _resolve_media_prompt_text(
        prompt_registry=prompt_registry,
        component_ids=component_ids,
        request=request,
        fallback_prompt=builtin_prompt,
    )
    return _build_multimodal_user_messages(
        prompt_text=prompt_text,
        instruction_text=instruction_text,
        asset=asset,
    )


def build_media_reanalysis_messages(
    *,
    instance_id: str,
    session_id: str,
    raw_hash: str,
    asset: dict[str, Any],
    question: str,
    model_context_window: int | None,
) -> list[dict[str, Any]]:
    """Build media reanalysis messages, falling back to builtin prompt when needed."""

    instruction_text = build_media_question_text(
        session_id=session_id,
        raw_hash=raw_hash,
        asset=asset,
        question=question,
    )
    return _build_multimodal_user_messages(
        prompt_text=MEDIA_REANALYSIS_SYSTEM_PROMPT,
        instruction_text=instruction_text,
        asset=asset,
    )


def build_media_inspection_instruction_text(
    *,
    session_id: str,
    raw_hash: str,
    asset: dict[str, Any],
    occurrence: dict[str, Any] | None,
) -> str:
    """Build the text instruction for one repeated-image inspection call."""

    count = int((occurrence or {}).get("occurrence_count") or 0)
    width = asset.get("width")
    height = asset.get("height")
    mime_type = str(asset.get("mime_type") or "")
    return (
        "请判断这张图片应归类为 generic_image、meme_image 或 emoji_native。\n"
        "这是一次会话内重复触发的媒体检定，请综合重复性与图像内容判断。\n"
        f"session_id={session_id}\n"
        f"raw_hash={raw_hash}\n"
        f"repeat_count_14d={count}\n"
        f"mime_type={mime_type or 'unknown'}\n"
        f"size={width or '?'}x{height or '?'}\n"
        "请始终返回 JSON，并将 digest 控制在 50 个汉字以内。"
    )


def build_sticker_summary_instruction_text(
    *,
    session_id: str,
    raw_hash: str,
    asset: dict[str, Any],
    occurrence: dict[str, Any] | None,
) -> str:
    """Build the text instruction for one custom-sticker inspection call."""

    count = int((occurrence or {}).get("occurrence_count") or 0)
    width = asset.get("width")
    height = asset.get("height")
    mime_type = str(asset.get("mime_type") or "")
    return (
        "请把这张图当作用户自定义表情或反应图来理解。\n"
        "重点描述其情绪、态度、动作、姿势、显著文字和可能的聊天语气。\n"
        "如果像普通图片而不像表情，也要如实说明。\n"
        f"session_id={session_id}\n"
        f"raw_hash={raw_hash}\n"
        f"repeat_count_14d={count}\n"
        f"mime_type={mime_type or 'unknown'}\n"
        f"size={width or '?'}x{height or '?'}\n"
        "请始终返回 JSON，并将 digest 控制在 50 个汉字以内。"
    )


def build_media_question_text(
    *,
    session_id: str,
    raw_hash: str,
    asset: dict[str, Any],
    question: str,
) -> str:
    """Build the reanalysis question text for one image."""

    width = asset.get("width")
    height = asset.get("height")
    mime_type = str(asset.get("mime_type") or "")
    return (
        "请把这张图片当作普通图像重新分析，并回答最后的问题。\n"
        f"session_id={session_id}\n"
        f"raw_hash={raw_hash}\n"
        f"mime_type={mime_type or 'unknown'}\n"
        f"size={width or '?'}x{height or '?'}\n"
        f"问题：{question.strip()}"
    )


def build_media_data_url(asset: dict[str, Any]) -> str:
    """Load one cached asset and convert it into a data URL."""

    storage_path = str(asset.get("storage_path") or "").strip()
    if not storage_path:
        raise FileNotFoundError("empty storage path")
    with open(storage_path, "rb") as file_obj:
        data = file_obj.read()
    mime_type = str(asset.get("mime_type") or "").strip() or "application/octet-stream"
    encoded = base64.b64encode(data).decode("ascii")
    return f"data:{mime_type};base64,{encoded}"


def _build_multimodal_user_messages(
    *,
    prompt_text: str,
    instruction_text: str,
    asset: dict[str, Any],
) -> list[dict[str, Any]]:
    text_parts = [part.strip() for part in (prompt_text, instruction_text) if part.strip()]
    return [
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "\n\n".join(text_parts)},
                {"type": "image_url", "image_url": {"url": build_media_data_url(asset)}},
            ],
        }
    ]


def _resolve_media_prompt_text(
    *,
    prompt_registry: Any,
    component_ids: list[str],
    request: PromptAssemblyRequest,
    fallback_prompt: str,
) -> str:
    rendered_parts: list[str] = []
    try:
        for component_id in component_ids:
            component = prompt_registry.get_component(component_id)
            if component is None or not component.enabled:
                continue
            rendered = render_component_text(
                component=component,
                request=request,
                source=infer_component_source(component),
                resolvers=getattr(prompt_registry, "_resolvers", {}),
            ).strip()
            if rendered:
                rendered_parts.append(rendered)
    except Exception:
        return fallback_prompt
    return "\n\n".join(rendered_parts) or fallback_prompt


def _resolve_prompt_component_ids(
    *,
    prompt_registry: Any,
    database: Any,
    prompt_refs: list[str],
    fallback_component_id: str,
) -> list[str]:
    component_ids: list[str] = []

    for prompt_ref in prompt_refs:
        normalized = str(prompt_ref or "").strip()
        if not normalized:
            continue
        component = prompt_registry.get_component(normalized)
        if component is not None:
            component_ids.append(component.id)
            continue

        payload = database.prompt_definitions.get(normalized)
        if payload is None:
            payload = database.prompt_definitions.get_by_prompt_id(normalized)
        if payload is None:
            continue
        component_ids.append(sync_prompt_definition_component(prompt_registry, payload))

    if not component_ids:
        return [fallback_component_id]

    seen: set[str] = set()
    deduped: list[str] = []
    for component_id in component_ids:
        if component_id not in seen:
            seen.add(component_id)
            deduped.append(component_id)
    return deduped
