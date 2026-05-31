from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from shinbot.admin.persona_files import PersonaFileRepository, render_persona_markdown
from shinbot.agent.coordinators.review.factory import ReviewRuntimeConfig, ReviewStageRuntimeConfig
from shinbot.agent.runners.review_scan import LLMReviewScanStageRunner
from shinbot.agent.runtime import install_agent_runtime
from shinbot.agent.runtime.config import (
    AgentRuntimeConfigError,
    agent_runtime_config_from_mapping,
    load_agent_runtime_config,
    validate_agent_runtime_config_mapping,
    validate_agent_runtime_config_references,
)
from shinbot.agent.scheduler import ActiveChatState, AgentScheduler, AgentState
from shinbot.agent.services.message_formatter import ImageMode
from shinbot.agent.services.model_runtime import GenerateResult
from shinbot.agent.services.prompt_engine import (
    PromptBuildRequest,
    PromptComponent,
    PromptComponentKind,
    PromptContextPolicy,
    PromptRegistry,
    PromptStage,
)
from shinbot.agent.signals import (
    AgentActiveChatBootstrapSignal,
    AgentMessageSignal,
    AgentSignal,
    AgentSignalKind,
    AgentSignalSource,
)
from shinbot.core.application.app import ShinBot
from shinbot.persistence import DatabaseManager
from shinbot.persistence.records import (
    InstanceConfigRecord,
    MessageLogRecord,
    ModelDefinitionRecord,
    ModelProviderRecord,
    ModelRouteMemberRecord,
    ModelRouteRecord,
)


class FakeModelRuntime:
    def __init__(self, responses: list[GenerateResult]) -> None:
        self.responses = list(responses)
        self.calls: list[Any] = []
        self.on_generate: Any | None = None

    async def generate(self, call: Any) -> GenerateResult:
        self.calls.append(call)
        if self.on_generate is not None:
            await self.on_generate(call)
        return self.responses.pop(0)


class RecordingWorkflowDispatcher:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def run_active_reply(
        self,
        *,
        session_id: str,
        message_log_id: int,
        sender_id: str,
        **kwargs: Any,
    ) -> None:
        self.calls.append(
            {
                "session_id": session_id,
                "message_log_id": message_log_id,
                "sender_id": sender_id,
                **kwargs,
            }
        )


def make_signal(
    *,
    message_log_id: int | None = 123,
    instance_id: str = "test-bot",
    bot_id: str = "",
    is_private: bool = False,
    is_mentioned: bool = False,
    is_reply_to_bot: bool = False,
) -> AgentSignal:
    message_token = message_log_id if message_log_id is not None else "missing"
    return AgentSignal(
        signal_id=f"message-ingress:test-bot:group:group:1:{message_token}",
        kind=AgentSignalKind.MESSAGE,
        source=AgentSignalSource.MESSAGE_INGRESS,
        session_id="test-bot:group:group:1",
        occurred_at=10.0,
        bot_id=bot_id,
        message=AgentMessageSignal(
            message_log_id=message_log_id,
            sender_id="user-1",
            instance_id=instance_id,
            platform="mock",
            self_id="bot-1",
            is_private=is_private,
            is_mentioned=is_mentioned,
            is_reply_to_bot=is_reply_to_bot,
        ),
        meta={"event_type": "message-created"},
    )


def make_tool_call(name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": f"call-{name}",
        "type": "function",
        "function": {
            "name": name,
            "arguments": json.dumps(arguments),
        },
    }


def make_generate_result(
    *,
    text: str = "",
    tool_calls: list[dict[str, Any]] | None = None,
) -> GenerateResult:
    return GenerateResult(
        text=text,
        tool_calls=list(tool_calls or []),
        raw_response={},
        execution_id="exec-active-chat",
        route_id="",
        provider_id="",
        model_id="",
        usage={},
    )


def seed_model_registry(db: DatabaseManager, *, route_id: str = "chat.default") -> None:
    db.model_registry.upsert_provider(
        ModelProviderRecord(
            id="test-provider",
            type="openai",
            display_name="Test Provider",
            auth={"api_key": "secret-key"},
        )
    )
    db.model_registry.upsert_model(
        ModelDefinitionRecord(
            id="test-provider/gpt-fast",
            provider_id="test-provider",
            backend_model="openai/gpt-4.1-mini",
            display_name="GPT Fast",
            capabilities=["chat"],
        )
    )
    db.model_registry.upsert_route(
        ModelRouteRecord(id=route_id, purpose="chat", strategy="priority"),
        members=[
            ModelRouteMemberRecord(
                route_id=route_id,
                model_id="test-provider/gpt-fast",
                priority=10,
            )
        ],
    )


def seed_persona(
    db: DatabaseManager,
    *,
    persona_id: str = "persona-test",
    prompt_uuid: str = "prompt-persona-test",
    prompt_id: str | None = None,
    stage: str = "identity",
    enabled: bool = True,
) -> str:
    del prompt_uuid, stage
    data_dir = db.config.sqlite_path.parent.parent
    persona_dir = data_dir / "personas"
    persona_dir.mkdir(parents=True, exist_ok=True)
    (persona_dir / f"{persona_id}.md").write_text(
        render_persona_markdown(
            persona_id=persona_id,
            name=f"Test Persona {persona_id}",
            prompt_text="You are the configured test persona.",
            tags=[],
            enabled=enabled,
            created_at="2026-01-01T00:00:00+00:00",
            updated_at="2026-01-01T00:00:00+00:00",
        ),
        encoding="utf-8",
    )
    return prompt_id or f"persona.{persona_id}"


def make_prompt_registry(
    *component_ids: str,
    stage: PromptStage = PromptStage.SYSTEM_BASE,
) -> PromptRegistry:
    registry = PromptRegistry()
    for component_id in component_ids:
        registry.register_component(
            PromptComponent(
                id=component_id,
                stage=stage,
                kind=PromptComponentKind.STATIC_TEXT,
                content=f"{component_id} content",
            )
        )
    return registry


class RecordingScheduler:
    def __init__(self) -> None:
        self.calls: list[AgentSignal] = []
        self.return_value: Any | None = None

    async def accept_signal(self, signal: AgentSignal) -> Any | None:
        self.calls.append(signal)
        return self.return_value


__all__ = [
    "ActiveChatState",
    "AgentActiveChatBootstrapSignal",
    "AgentMessageSignal",
    "AgentRuntimeConfigError",
    "AgentScheduler",
    "AgentSignal",
    "AgentSignalKind",
    "AgentSignalSource",
    "AgentState",
    "Any",
    "DatabaseManager",
    "FakeModelRuntime",
    "GenerateResult",
    "ImageMode",
    "InstanceConfigRecord",
    "LLMReviewScanStageRunner",
    "MessageLogRecord",
    "ModelDefinitionRecord",
    "ModelProviderRecord",
    "ModelRouteMemberRecord",
    "ModelRouteRecord",
    "Path",
    "PersonaFileRepository",
    "PromptBuildRequest",
    "PromptComponent",
    "PromptComponentKind",
    "PromptContextPolicy",
    "PromptRegistry",
    "PromptStage",
    "RecordingScheduler",
    "RecordingWorkflowDispatcher",
    "ReviewRuntimeConfig",
    "ReviewStageRuntimeConfig",
    "ShinBot",
    "agent_runtime_config_from_mapping",
    "annotations",
    "asyncio",
    "install_agent_runtime",
    "json",
    "load_agent_runtime_config",
    "make_generate_result",
    "make_prompt_registry",
    "make_signal",
    "make_tool_call",
    "pytest",
    "render_persona_markdown",
    "seed_model_registry",
    "seed_persona",
    "validate_agent_runtime_config_mapping",
    "validate_agent_runtime_config_references",
]
