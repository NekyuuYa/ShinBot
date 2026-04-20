"""Built-in context strategy helpers for prompt management."""

from __future__ import annotations

import math
from typing import Any

from shinbot.agent.prompt_manager.schema import ContextStrategy, PromptAssemblyRequest


def _resolve_effective_context_window(
    strategy_max_context_tokens: int | None,
    model_context_window: int | None,
) -> int | None:
    if strategy_max_context_tokens is not None and model_context_window is not None:
        return min(strategy_max_context_tokens, model_context_window)
    return strategy_max_context_tokens or model_context_window


def hydrate_request_context(
    context_manager: Any,
    request: PromptAssemblyRequest,
) -> PromptAssemblyRequest:
    """Hydrate context inputs from the active context manager when available."""

    if context_manager is None or not request.session_id or not request.hydrate_session_context:
        return request
    context_inputs = context_manager.get_context_inputs(
        request.session_id,
        fallback=request.context_inputs,
    )
    return request.model_copy(update={"context_inputs": context_inputs})


def sync_context_policy(
    context_manager: Any,
    request: PromptAssemblyRequest,
    strategy: ContextStrategy,
) -> dict[str, Any]:
    """Sync session-level context trimming policy before resolver execution."""

    if context_manager is None or not request.session_id or not request.hydrate_session_context:
        return {}
    return context_manager.set_session_policy(
        request.session_id,
        strategy=strategy,
        model_context_window=request.model_context_window,
    )


def resolve_builtin_sliding_window_context(
    *,
    context_manager: Any,
    request: PromptAssemblyRequest,
    strategy: ContextStrategy,
) -> dict[str, Any]:
    """Resolve the built-in sliding-window context strategy."""

    turns = normalize_history_turns(request.context_inputs)
    summary = str(request.context_inputs.get("summary", "")).strip()
    max_context_tokens = _resolve_effective_context_window(
        strategy.budget.max_context_tokens,
        request.model_context_window,
    )
    trigger_ratio = strategy.budget.trigger_ratio
    target_context_tokens = strategy.budget.target_context_tokens
    trim_ratio = strategy.budget.trim_ratio
    trim_turns = strategy.budget.trim_turns
    dropped_turns = 0

    if (
        strategy.budget.max_history_turns is not None
        and len(turns) > strategy.budget.max_history_turns
    ):
        overflow = len(turns) - strategy.budget.max_history_turns
        turns = turns[overflow:]
        dropped_turns += overflow

    trigger_tokens = (
        max(1, math.floor(max_context_tokens * trigger_ratio))
        if max_context_tokens is not None
        else None
    )
    effective_target_tokens = (
        target_context_tokens
        if target_context_tokens is not None
        and trigger_tokens is not None
        and target_context_tokens < trigger_tokens
        else None
    )

    if context_manager is not None and request.session_id and request.hydrate_session_context:
        ejection = context_manager.apply_batch_ejection(
            request.session_id,
            strategy=strategy,
            model_context_window=request.model_context_window,
        )
        dropped_turns = int(ejection.get("dropped_turns", 0))
        turns = normalize_history_turns(
            context_manager.get_context_inputs(
                request.session_id,
                fallback={"summary": summary},
            )
        )
    else:
        while len(turns) > 1:
            current_tokens = estimate_context_tokens(turns, summary)

            if effective_target_tokens is not None:
                if current_tokens <= effective_target_tokens:
                    break
                turns = turns[1:]
                dropped_turns += 1
                continue

            if trigger_tokens is None or current_tokens < trigger_tokens:
                break

            trim_count = (
                max(1, math.floor(len(turns) * trim_ratio))
                if trim_ratio is not None
                else max(1, trim_turns)
            )
            trim_count = min(trim_count, len(turns) - 1)
            turns = turns[trim_count:]
            dropped_turns += trim_count

    messages: list[dict[str, Any]] = []
    if summary:
        messages.append({"role": "user", "content": f"[Summary]\n{summary}"})
    for turn in turns:
        role = turn.get("role", "user") or "user"
        message: dict[str, Any] = {
            "role": role,
            "content": turn["content"],
        }
        sender_id = str(turn.get("sender_id", "") or "").strip()
        if sender_id:
            message["sender_id"] = sender_id
        messages.append(message)

    return {
        "messages": messages,
        "dropped_turns": dropped_turns,
        "trigger_tokens": trigger_tokens,
        "target_tokens": effective_target_tokens,
        "remaining_turns": len(turns),
    }


def normalize_history_turns(context_inputs: dict[str, Any]) -> list[dict[str, Any]]:
    """Normalize heterogeneous history_turn inputs into registry turn objects."""

    raw_turns = context_inputs.get("history_turns", [])
    if not isinstance(raw_turns, list):
        return []

    turns: list[dict[str, Any]] = []
    for item in raw_turns:
        if isinstance(item, str):
            content = item.strip()
            if content:
                turns.append({"role": "", "content": content})
            continue
        if not isinstance(item, dict):
            continue
        role = str(item.get("role", "")).strip()
        raw_content = item.get("content", "")
        if isinstance(raw_content, list):
            content_parts: list[str] = []
            for block in raw_content:
                if isinstance(block, dict):
                    block_text = block.get("text")
                    if isinstance(block_text, str) and block_text.strip():
                        content_parts.append(block_text.strip())
            content = "\n".join(content_parts).strip()
        else:
            content = str(raw_content).strip()
        if not content:
            continue
        turn: dict[str, Any] = {"role": role, "content": content}
        sender_id = str(
            item.get("sender_id", item.get("senderId", item.get("name", ""))) or ""
        ).strip()
        if sender_id:
            turn["sender_id"] = sender_id
        sender_name = str(item.get("sender_name", item.get("senderName", "")) or "").strip()
        if sender_name:
            turn["sender_name"] = sender_name
        platform = str(item.get("platform", "") or "").strip()
        if platform:
            turn["platform"] = platform
        turns.append(turn)
    return turns


def estimate_context_tokens(turns: list[dict[str, Any]], summary: str) -> int:
    """Estimate context token usage for the built-in sliding-window strategy."""

    text_parts = [summary] if summary else []
    text_parts.extend(
        f"{turn['role']}: {turn['content']}" if turn["role"] else turn["content"] for turn in turns
    )
    text = "\n".join(part for part in text_parts if part).strip()
    if not text:
        return 0
    word_estimate = len(text.split())
    char_estimate = math.ceil(len(text) / 4)
    return max(word_estimate, char_estimate)
