"""Tests for media fingerprinting and inspection config resolution."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from PIL import Image

from shinbot.admin.prompt_definition_admin import (
    PromptDefinitionFileRepository,
    normalize_prompt_definition_input,
)
from shinbot.agent.services.context.builders.message_parts import parse_message_parts
from shinbot.agent.services.media import (
    BUILTIN_MEDIA_INSPECTION_AGENT_REF,
    BUILTIN_MEDIA_INSPECTION_LLM_REF,
    BUILTIN_MEDIA_INSPECTION_PROMPT_ID,
    MediaIngressHook,
    MediaInspectionRunner,
    MediaService,
    register_media_tools,
    resolve_media_inspection_config,
)
from shinbot.agent.services.media.prompt_building import build_media_reanalysis_messages
from shinbot.agent.services.prompt_engine import PromptRegistry
from shinbot.agent.services.tools import ToolCallRequest, ToolManager, ToolRegistry
from shinbot.agent.utils.workflow_formatting import (
    format_incremental_messages,
    format_message_line,
)
from shinbot.core.dispatch.ingress import MessageIngress
from shinbot.core.dispatch.routing import RouteTable
from shinbot.core.platform.adapter_manager import BaseAdapter, MessageHandle
from shinbot.core.security.permission import PermissionEngine
from shinbot.core.state.session import SessionManager
from shinbot.persistence import (
    DatabaseManager,
    InstanceConfigRecord,
    MediaSemanticRecord,
    MessageLogRecord,
    ModelDefinitionRecord,
    ModelProviderRecord,
    ModelRouteMemberRecord,
    ModelRouteRecord,
)
from shinbot.persistence.records import utc_now_iso
from shinbot.schema.elements import Message, MessageElement
from shinbot.schema.events import MessagePayload, UnifiedEvent
from shinbot.schema.resources import Channel, User


class MockAdapter(BaseAdapter):
    def __init__(self, instance_id: str = "test-bot", platform: str = "mock", **kwargs):
        super().__init__(instance_id, platform)
        self.sent: list[tuple[str, list[MessageElement]]] = []

    async def start(self):
        pass

    async def shutdown(self):
        pass

    async def send(self, target_session, elements):
        self.sent.append((target_session, elements))
        return MessageHandle(message_id=f"sent-{len(self.sent)}", adapter_ref=self)

    async def call_api(self, method, params):
        return {"ok": True}

    async def get_capabilities(self):
        return {"elements": ["text", "img"], "actions": [], "limits": {}}


def _make_group_event(
    content: str,
    *,
    user_id: str = "user-1",
    message_id: str = "msg-1",
) -> UnifiedEvent:
    return UnifiedEvent(
        type="message-created",
        self_id="bot-1",
        platform="mock",
        user=User(id=user_id, name="Tester"),
        channel=Channel(id="group:1", type=0),
        message=MessagePayload(id=message_id, content=content),
    )


def _write_png(path: Path, color: tuple[int, int, int] = (255, 0, 0)) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", (8, 8), color).save(path)
    return path


def _make_media_ingress(
    tmp_path: Path,
    db: DatabaseManager,
    *,
    media_service: MediaService,
    media_inspection_runner=None,
) -> MessageIngress:
    ingress = MessageIngress(
        session_manager=SessionManager(data_dir=tmp_path, session_repo=db.sessions),
        permission_engine=PermissionEngine(),
        route_table=RouteTable(),
        database=db,
    )
    ingress.add_pre_route_hook(MediaIngressHook(media_service, media_inspection_runner))
    return ingress


def _seed_media_runtime(
    db: DatabaseManager,
    *,
    instance_id: str,
    llm_ref: str = "route.media.inspect",
    media_prompt_ref: str | None = None,
    sticker_llm_ref: str | None = None,
    sticker_prompt_ref: str | None = None,
) -> None:
    now = utc_now_iso()
    provider_id = "openai-media"
    model_id = "openai-media/gpt-vision"
    route_id = "route.media.inspect"
    config: dict[str, str] = {"media_inspection_llm": llm_ref}
    if media_prompt_ref is not None:
        config["media_inspection_prompt"] = media_prompt_ref
    if sticker_llm_ref is not None:
        config["sticker_summary_llm"] = sticker_llm_ref
    if sticker_prompt_ref is not None:
        config["sticker_summary_prompt"] = sticker_prompt_ref

    db.model_registry.upsert_provider(
        ModelProviderRecord(
            id=provider_id,
            type="openai",
            display_name="OpenAI Media",
            base_url="https://api.openai.com/v1",
            auth={"api_key": "secret-key"},
        )
    )
    db.model_registry.upsert_model(
        ModelDefinitionRecord(
            id=model_id,
            provider_id=provider_id,
            litellm_model="openai/gpt-4.1-mini",
            display_name="GPT Vision",
            capabilities=["chat"],
            context_window=64000,
        )
    )
    db.model_registry.upsert_route(
        ModelRouteRecord(id=route_id, purpose="media_inspection", strategy="priority"),
        members=[
            ModelRouteMemberRecord(
                route_id=route_id,
                model_id=model_id,
                priority=10,
                weight=1.0,
            )
        ],
    )
    if sticker_llm_ref is not None and sticker_llm_ref != route_id:
        db.model_registry.upsert_route(
            ModelRouteRecord(id=sticker_llm_ref, purpose="sticker_summary", strategy="priority"),
            members=[
                ModelRouteMemberRecord(
                    route_id=sticker_llm_ref,
                    model_id=model_id,
                    priority=10,
                    weight=1.0,
                )
            ],
        )
    db.instance_configs.upsert(
        InstanceConfigRecord(
            uuid="instance-config-media",
            instance_id=instance_id,
            main_llm=route_id,
            config=config,
            created_at=now,
            updated_at=now,
        )
    )


def _seed_main_runtime(
    db: DatabaseManager,
    *,
    instance_id: str,
    media_llm_ref: str = "",
) -> str:
    now = utc_now_iso()
    provider_id = "openai-main"
    model_id = "openai-main/gpt-chat"
    route_id = "route.main.chat"
    config: dict[str, str] = {}
    if media_llm_ref:
        config["media_inspection_llm"] = media_llm_ref

    db.model_registry.upsert_provider(
        ModelProviderRecord(
            id=provider_id,
            type="openai",
            display_name="OpenAI Main",
            base_url="https://api.openai.com/v1",
            auth={"api_key": "secret-key"},
        )
    )
    db.model_registry.upsert_model(
        ModelDefinitionRecord(
            id=model_id,
            provider_id=provider_id,
            litellm_model="openai/gpt-4.1",
            display_name="GPT Chat",
            capabilities=["chat"],
            context_window=128000,
        )
    )
    db.model_registry.upsert_route(
        ModelRouteRecord(id=route_id, purpose="chat", strategy="priority"),
        members=[
            ModelRouteMemberRecord(
                route_id=route_id,
                model_id=model_id,
                priority=10,
                weight=1.0,
            )
        ],
    )
    db.instance_configs.upsert(
        InstanceConfigRecord(
            uuid="instance-config-main",
            instance_id=instance_id,
            main_llm=route_id,
            config=config,
            created_at=now,
            updated_at=now,
        )
    )
    return route_id


def _seed_custom_media_prompt(db: DatabaseManager, *, prompt_ref: str) -> None:
    data_dir = db.config.sqlite_path.parent.parent
    PromptDefinitionFileRepository.from_data_dir(data_dir).create(
        normalize_prompt_definition_input(
            prompt_id=prompt_ref,
            name="Custom Media Inspector",
            source_type="user_defined",
            source_id="tests",
            owner_plugin_id="",
            owner_module="",
            module_path="",
            stage="system_base",
            type="static_text",
            priority=100,
            version="1.0.0",
            description="",
            enabled=True,
            content="You are a custom media inspector.",
            template_vars=[],
            resolver_ref="",
            bundle_refs=[],
            config={},
            tags=[],
            metadata={},
        )
    )


class FakeModelRuntime:
    def __init__(self, response_text: str) -> None:
        self.response_text = response_text
        self.calls = []

    async def generate(self, call):
        self.calls.append(call)
        return type(
            "Result",
            (),
            {
                "text": self.response_text,
                "execution_id": "exec-media",
                "route_id": call.route_id or "",
                "provider_id": "provider",
                "model_id": call.model_id or "",
                "usage": {},
            },
        )()


class FakeInspectionRunner:
    def __init__(self) -> None:
        self.calls = []

    def schedule_items(self, *, instance_id: str, session_id: str, items) -> None:
        self.calls.append(
            {
                "instance_id": instance_id,
                "session_id": session_id,
                "items": items,
            }
        )

    async def answer_question(
        self,
        *,
        instance_id: str,
        session_id: str,
        raw_hash: str,
        question: str,
    ) -> dict[str, str]:
        self.calls.append(
            {
                "instance_id": instance_id,
                "session_id": session_id,
                "raw_hash": raw_hash,
                "question": question,
            }
        )
        return {
            "raw_hash": raw_hash,
            "answer": f"回答: {question}",
            "inspection_agent_ref": BUILTIN_MEDIA_INSPECTION_AGENT_REF,
            "inspection_llm_ref": BUILTIN_MEDIA_INSPECTION_LLM_REF,
        }


__all__ = [
    "BUILTIN_MEDIA_INSPECTION_AGENT_REF",
    "BUILTIN_MEDIA_INSPECTION_LLM_REF",
    "BUILTIN_MEDIA_INSPECTION_PROMPT_ID",
    "BUILTIN_MEDIA_INSPECTION_PROMPT_ID",
    "BaseAdapter",
    "Channel",
    "DatabaseManager",
    "FakeInspectionRunner",
    "FakeModelRuntime",
    "Image",
    "InstanceConfigRecord",
    "MediaIngressHook",
    "MediaInspectionRunner",
    "MediaSemanticRecord",
    "MediaService",
    "Message",
    "MessageElement",
    "MessageHandle",
    "MessageIngress",
    "MessageLogRecord",
    "MessagePayload",
    "MockAdapter",
    "ModelDefinitionRecord",
    "ModelProviderRecord",
    "ModelRouteMemberRecord",
    "ModelRouteRecord",
    "Path",
    "PermissionEngine",
    "PromptRegistry",
    "RouteTable",
    "SessionManager",
    "ToolCallRequest",
    "ToolManager",
    "ToolRegistry",
    "UnifiedEvent",
    "User",
    "_make_group_event",
    "_make_media_ingress",
    "_seed_custom_media_prompt",
    "_seed_main_runtime",
    "_seed_media_runtime",
    "_write_png",
    "annotations",
    "build_media_reanalysis_messages",
    "format_incremental_messages",
    "format_message_line",
    "json",
    "parse_message_parts",
    "pytest",
    "register_media_tools",
    "resolve_media_inspection_config",
    "utc_now_iso",
]
