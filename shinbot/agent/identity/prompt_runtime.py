"""Identity-specific runtime prompt helpers."""

from __future__ import annotations

import re
from typing import Any

from shinbot.agent.prompt_manager.context_strategies import normalize_history_turns
from shinbot.agent.prompt_manager.schema import (
    PromptAssemblyRequest,
    PromptComponent,
    PromptSource,
    stable_text_hash,
)


def resolve_identity_map_prompt(
    *,
    identity_store: Any,
    request: PromptAssemblyRequest,
    _component: PromptComponent,
    _source: PromptSource,
) -> dict[str, Any]:
    """Build the dynamic identity map prompt payload for current participants."""

    turns = normalize_history_turns(request.context_inputs)
    identity_turns = request.context_inputs.get("identity_turns", [])
    if identity_turns:
        turns.extend(normalize_history_turns({"history_turns": identity_turns}))
    active_participants: dict[str, dict[str, str]] = {}
    for turn in turns:
        if str(turn.get("role", "")).strip() != "user":
            continue
        sender_id = str(turn.get("sender_id", "") or "").strip()
        if not sender_id:
            continue
        participant = active_participants.setdefault(
            sender_id,
            {"sender_name": "", "platform": ""},
        )
        sender_name = str(turn.get("sender_name", "") or "").strip()
        if sender_name and not participant["sender_name"]:
            participant["sender_name"] = sender_name
        platform = str(turn.get("platform", "") or "").strip()
        if platform and not participant["platform"]:
            participant["platform"] = platform

    if not active_participants:
        return {"text": "", "active_user_ids": [], "mapped_user_ids": []}

    context_platform = str(request.context_inputs.get("platform", "") or "").strip()
    mapped_lines: list[str] = []
    mapped_ids: list[str] = []

    for user_id, participant in active_participants.items():
        lookup_platform = context_platform or participant["platform"]
        identity = None
        if identity_store is not None:
            identity = identity_store.get_identity(user_id, platform=lookup_platform)
            if identity is None:
                identity_store.ensure_user(
                    user_id=user_id,
                    suggested_name=participant["sender_name"],
                    platform=lookup_platform,
                )
                identity = identity_store.get_identity(user_id, platform=lookup_platform)

        if identity is None:
            continue

        display_name = str(identity.get("name", "")).strip()
        if not display_name:
            continue

        aliases = identity.get("aname", [])
        if isinstance(aliases, str):
            alias_list = [aliases.strip()] if aliases.strip() else []
        elif isinstance(aliases, list):
            alias_list = [str(alias).strip() for alias in aliases if str(alias).strip()]
        else:
            alias_list = []

        note = str(identity.get("note", "")).strip()
        line = f"- ID: {user_id} -> 昵称: {display_name}"
        if alias_list:
            line += f" 别名: {'/'.join(alias_list)}"
        if note:
            line += f" (备注: {note})"
        mapped_lines.append(line)
        mapped_ids.append(user_id)

    if not mapped_lines:
        return {
            "text": "",
            "active_user_ids": list(active_participants.keys()),
            "mapped_user_ids": [],
        }

    text_lines = [
        "### 参与者身份参考 (Identity Map)",
        "以下是当前对话参与者的 ID 与你应当称呼他们的“昵称”映射：",
        *mapped_lines,
    ]
    return {
        "text": "\n".join(text_lines).strip(),
        "active_user_ids": list(active_participants.keys()),
        "mapped_user_ids": mapped_ids,
    }


def inject_identity_layers_into_messages(
    messages: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Normalize user message sender identity hints for prompt consumption."""

    enriched: list[dict[str, Any]] = []
    for message in messages:
        if not isinstance(message, dict):
            continue

        payload = dict(message)
        role = str(payload.get("role", "")).strip()
        sender_id = str(payload.get("sender_id", "")).strip()

        if role == "user" and sender_id:
            payload["name"] = _format_sender_name(sender_id)
            content = payload.get("content")
            if isinstance(content, str):
                payload["content"] = _prefix_sender_id_inline(content, sender_id)

        payload.pop("sender_id", None)
        payload.pop("sender_name", None)
        payload.pop("platform", None)
        enriched.append(payload)

    return enriched


def _prefix_sender_id_inline(content: str, sender_id: str) -> str:
    body = content.strip()
    if not body:
        return body
    marker = f"【{sender_id}】"
    if body.startswith(marker):
        return body
    return f"{marker}{body}"


def _format_sender_name(sender_id: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9_-]", "_", sender_id.strip())
    normalized = normalized.strip("_")
    if not normalized:
        normalized = stable_text_hash(sender_id)[:12]
    if normalized[0].isdigit():
        normalized = f"u_{normalized}"
    return normalized[:64]
