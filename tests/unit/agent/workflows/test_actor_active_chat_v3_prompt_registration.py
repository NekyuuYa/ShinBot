"""Tests for the Actor v3 Active Chat prompt contract."""

from __future__ import annotations

import pytest

from shinbot.agent.services.prompt_engine import (
    PromptBuildRequest,
    PromptContextPolicy,
    PromptFileLoadConfig,
    PromptRegistry,
)
from shinbot.agent.workflows.active_chat.prompt_registration import (
    ACTOR_ACTIVE_CHAT_V3_PROMPT_COMPONENT_IDS,
    register_active_chat_prompt_components,
)


def _prompt_text(messages: list[dict[str, object]]) -> str:
    """Return all text blocks from a parsed prompt message list."""

    text_blocks: list[str] = []
    for message in messages:
        content = message.get("content")
        if not isinstance(content, list):
            continue
        for block in content:
            if isinstance(block, dict) and isinstance(block.get("text"), str):
                text_blocks.append(block["text"])
    return "\n".join(text_blocks)


@pytest.mark.parametrize(
    ("locale", "required_phrases", "legacy_phrase"),
    [
        (
            "zh-CN",
            (
                "必须且只能产生一个终止工具调用",
                "`no_reply`、`exit_active`、一条 `send_reply`，或一条 `send_reaction`",
                "`quote_message_log_id`",
                "`message_log_id` 只能是本回合上下文中列出的已选择持久化",
                "`send_poke`",
                "组合多个 action",
                "平台消息 ID",
            ),
            "一个或多个 send_reply",
        ),
        (
            "en-US",
            (
                "exactly one terminal tool call",
                "`no_reply`, `exit_active`, one `send_reply`, or one `send_reaction`",
                "`quote_message_log_id`",
                "`message_log_id` must be one of the selected durable",
                "`send_poke`",
                "compose multiple actions",
                "platform message ID",
            ),
            "one or more send_reply",
        ),
    ],
)
def test_actor_active_chat_v3_round_uses_its_own_bound_terminal_contract(
    locale: str,
    required_phrases: tuple[str, ...],
    legacy_phrase: str,
) -> None:
    """The Actor v3 map loads a strict prompt without legacy constraints."""

    registry = PromptRegistry()
    register_active_chat_prompt_components(
        registry,
        prompt_file_config=PromptFileLoadConfig(locale=locale),
    )

    result = registry.build_messages(
        PromptBuildRequest(
            caller="agent.active_chat.round",
            workflow_id="active_chat_round",
            stage_id="active_chat_round",
            component_ids_by_stage=ACTOR_ACTIVE_CHAT_V3_PROMPT_COMPONENT_IDS[
                "round"
            ],
            context_policy=PromptContextPolicy.DISABLED,
        )
    )

    component_ids = [record.component_id for record in result.ordered_components]
    prompt_text = _prompt_text(result.messages)

    assert component_ids == [
        "active_chat.actor_v3.round.system",
        "active_chat.actor_v3.round.constraints",
    ]
    assert "active_chat.fast_mode.constraints" not in component_ids
    assert legacy_phrase not in prompt_text
    for phrase in required_phrases:
        assert phrase in prompt_text
