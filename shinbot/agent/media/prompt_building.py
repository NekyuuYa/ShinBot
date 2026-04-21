"""Prompt/message construction helpers for media inspection workflows."""

from __future__ import annotations

import base64
from typing import Any

from shinbot.agent.media.config import (
    BUILTIN_MEDIA_INSPECTION_PROMPT,
    BUILTIN_STICKER_SUMMARY_PROMPT,
)
from shinbot.agent.prompt_manager import PromptAssemblyRequest

MEDIA_REANALYSIS_SYSTEM_PROMPT = """
You are ShinBot's media reanalysis agent.

Answer the user's question about the supplied image as a normal image understanding task.
Describe only what is visibly supported by the image.
If identity, source character, or text content is uncertain, say so explicitly.
Prefer concise Chinese answers that are useful inside a chat workflow.
""".strip()


def build_media_inspection_messages(
    *,
    resolved_agent_ref: str,
    resolved_llm_ref: str,
    uses_builtin_agent: bool,
    prompt_registry: Any,
    database: Any,
    instance_id: str,
    session_id: str,
    raw_hash: str,
    asset: dict[str, Any],
    occurrence: dict[str, Any] | None,
    model_context_window: int | None,
    resolve_agent: Any,
    resolve_model_target: Any,
    build_component_ids: Any,
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
        instruction_text=instruction_text,
        resolved_agent_ref=resolved_agent_ref,
        resolved_llm_ref=resolved_llm_ref,
        uses_builtin_agent=uses_builtin_agent,
        prompt_registry=prompt_registry,
        database=database,
        instance_id=instance_id,
        session_id=session_id,
        raw_hash=raw_hash,
        asset=asset,
        model_context_window=model_context_window,
        resolve_agent=resolve_agent,
        resolve_model_target=resolve_model_target,
        build_component_ids=build_component_ids,
    )


def build_sticker_summary_messages(
    *,
    resolved_agent_ref: str,
    resolved_llm_ref: str,
    uses_builtin_agent: bool,
    prompt_registry: Any,
    database: Any,
    instance_id: str,
    session_id: str,
    raw_hash: str,
    asset: dict[str, Any],
    occurrence: dict[str, Any] | None,
    model_context_window: int | None,
    resolve_agent: Any,
    resolve_model_target: Any,
    build_component_ids: Any,
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
        instruction_text=instruction_text,
        resolved_agent_ref=resolved_agent_ref,
        resolved_llm_ref=resolved_llm_ref,
        uses_builtin_agent=uses_builtin_agent,
        prompt_registry=prompt_registry,
        database=database,
        instance_id=instance_id,
        session_id=session_id,
        raw_hash=raw_hash,
        asset=asset,
        model_context_window=model_context_window,
        resolve_agent=resolve_agent,
        resolve_model_target=resolve_model_target,
        build_component_ids=build_component_ids,
    )


def _build_media_prompt_messages(
    *,
    builtin_prompt: str,
    instruction_text: str,
    resolved_agent_ref: str,
    resolved_llm_ref: str,
    uses_builtin_agent: bool,
    prompt_registry: Any,
    database: Any,
    instance_id: str,
    session_id: str,
    raw_hash: str,
    asset: dict[str, Any],
    model_context_window: int | None,
    resolve_agent: Any,
    resolve_model_target: Any,
    build_component_ids: Any,
) -> list[dict[str, Any]]:
    multimodal_user_message = {
        "role": "user",
        "content": [
            {"type": "text", "text": instruction_text},
            {"type": "image_url", "image_url": {"url": build_media_data_url(asset)}},
        ],
    }

    if uses_builtin_agent:
        return [
            {
                "role": "system",
                "content": [{"type": "text", "text": builtin_prompt}],
            },
            multimodal_user_message,
        ]

    agent = resolve_agent(resolved_agent_ref)
    if agent is None:
        return _builtin_messages(builtin_prompt, multimodal_user_message)

    persona_uuid = str(agent.get("persona_uuid") or "").strip()
    persona = database.personas.get(persona_uuid) if persona_uuid else None
    if persona is None:
        return _builtin_messages(builtin_prompt, multimodal_user_message)

    route_id, model_id, _, _ = resolve_model_target(
        instance_id=instance_id,
        llm_ref=resolved_llm_ref,
    )
    component_ids = build_component_ids(agent, persona)
    request = PromptAssemblyRequest(
        caller="media.inspection_runner",
        session_id=session_id,
        instance_id=instance_id,
        route_id=route_id,
        model_id=model_id,
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
            "trigger": "media_inspection",
            "agent_uuid": str(agent.get("uuid") or ""),
            "persona_uuid": persona_uuid,
            "raw_hash": raw_hash,
        },
    )
    try:
        assembly = prompt_registry.assemble(request)
    except Exception:
        return _builtin_messages(builtin_prompt, multimodal_user_message)
    return [*assembly.messages, multimodal_user_message]


def build_media_reanalysis_messages(
    *,
    resolved_agent_ref: str,
    resolved_llm_ref: str,
    uses_builtin_agent: bool,
    prompt_registry: Any,
    database: Any,
    instance_id: str,
    session_id: str,
    raw_hash: str,
    asset: dict[str, Any],
    question: str,
    model_context_window: int | None,
    resolve_agent: Any,
    resolve_model_target: Any,
    build_component_ids: Any,
) -> list[dict[str, Any]]:
    """Build media reanalysis messages, falling back to builtin prompt when needed."""

    instruction_text = build_media_question_text(
        session_id=session_id,
        raw_hash=raw_hash,
        asset=asset,
        question=question,
    )
    multimodal_user_message = {
        "role": "user",
        "content": [
            {"type": "text", "text": instruction_text},
            {"type": "image_url", "image_url": {"url": build_media_data_url(asset)}},
        ],
    }

    if uses_builtin_agent:
        return _builtin_messages(MEDIA_REANALYSIS_SYSTEM_PROMPT, multimodal_user_message)

    agent = resolve_agent(resolved_agent_ref)
    if agent is None:
        return _builtin_messages(MEDIA_REANALYSIS_SYSTEM_PROMPT, multimodal_user_message)

    persona_uuid = str(agent.get("persona_uuid") or "").strip()
    persona = database.personas.get(persona_uuid) if persona_uuid else None
    if persona is None:
        return _builtin_messages(MEDIA_REANALYSIS_SYSTEM_PROMPT, multimodal_user_message)

    route_id, model_id, _, _ = resolve_model_target(
        instance_id=instance_id,
        llm_ref=resolved_llm_ref,
    )
    component_ids = build_component_ids(agent, persona)
    request = PromptAssemblyRequest(
        caller="media.reanalysis_runner",
        session_id=session_id,
        instance_id=instance_id,
        route_id=route_id,
        model_id=model_id,
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
            "trigger": "media_reanalysis",
            "agent_uuid": str(agent.get("uuid") or ""),
            "persona_uuid": persona_uuid,
            "raw_hash": raw_hash,
        },
    )
    try:
        assembly = prompt_registry.assemble(request)
    except Exception:
        return _builtin_messages(MEDIA_REANALYSIS_SYSTEM_PROMPT, multimodal_user_message)
    return [*assembly.messages, multimodal_user_message]


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


def _builtin_messages(
    system_prompt: str,
    multimodal_user_message: dict[str, Any],
) -> list[dict[str, Any]]:
    return [
        {
            "role": "system",
            "content": [{"type": "text", "text": system_prompt}],
        },
        multimodal_user_message,
    ]
