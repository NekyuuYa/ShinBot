from __future__ import annotations

import base64
import hashlib
import json

import pytest

from shinbot.agent.services.model_runtime import ModelCallError, ModelRuntimeCall
from shinbot.agent.services.model_runtime.backends import BackendRequestPlan
from shinbot.agent.services.model_runtime.planning import build_litellm_kwargs
from shinbot.agent.services.model_runtime.service import ModelRuntime
from shinbot.persistence import DatabaseManager
from shinbot.persistence.records import (
    ModelDefinitionRecord,
    ModelProviderRecord,
    ModelRouteMemberRecord,
    ModelRouteRecord,
)


class RecordingBackend:
    """Test backend that records plans and returns OpenAI-compatible payloads."""

    name = "recording"

    def __init__(self) -> None:
        self.plans: list[BackendRequestPlan] = []

    def plan_request(
        self,
        *,
        provider: dict[str, object],
        model: dict[str, object],
        call: ModelRuntimeCall,
        timeout_override: float | None,
        operation: str,
    ) -> BackendRequestPlan:
        payload = {
            "model": model["backend_model"],
            "messages": list(call.messages),
            "timeout": timeout_override,
            **dict(call.params),
        }
        return BackendRequestPlan(
            operation=operation,  # type: ignore[arg-type]
            payload=payload,
            safe_payload=dict(payload),
            backend_name=self.name,
            backend_model=str(payload["model"]),
        )

    def invoke(self, plan: BackendRequestPlan) -> dict[str, object]:
        self.plans.append(plan)
        return {
            "model": plan.backend_model,
            "choices": [{"message": {"content": "hello from backend"}}],
            "usage": {"prompt_tokens": 2, "completion_tokens": 3},
        }

    def normalize_response(
        self,
        *,
        operation: str,
        response: object,
        usage: dict[str, object],
    ) -> dict[str, object]:
        from shinbot.agent.services.model_runtime.extraction import (
            extract_text,
            extract_tool_calls_list,
        )

        return {
            "text": extract_text(response),
            "tool_calls": extract_tool_calls_list(response),
        }


def _seed_runtime(db: DatabaseManager) -> None:
    db.model_registry.upsert_provider(
        ModelProviderRecord(
            id="openai-main",
            type="openai",
            display_name="OpenAI Main",
            base_url="https://api.openai.com/v1",
            auth={"api_key": "secret-key"},
            default_params={"temperature": 0.2},
        )
    )
    db.model_registry.upsert_model(
        ModelDefinitionRecord(
            id="openai-main/gpt-fast",
            provider_id="openai-main",
            backend_model="openai/gpt-4.1-mini",
            display_name="GPT Fast",
            capabilities=["chat"],
            default_params={"max_tokens": 128},
        )
    )
    db.model_registry.upsert_model(
        ModelDefinitionRecord(
            id="openai-main/gpt-backup",
            provider_id="openai-main",
            backend_model="openai/gpt-4.1",
            display_name="GPT Backup",
            capabilities=["chat"],
        )
    )
    db.model_registry.upsert_model(
        ModelDefinitionRecord(
            id="openai-main/text-embedding",
            provider_id="openai-main",
            backend_model="openai/text-embedding-3-small",
            display_name="Embedding",
            capabilities=["embedding"],
        )
    )
    db.model_registry.upsert_route(
        ModelRouteRecord(id="agent.default_chat", purpose="chat", strategy="priority"),
        members=[
            ModelRouteMemberRecord(
                route_id="agent.default_chat",
                model_id="openai-main/gpt-fast",
                priority=10,
                weight=1.0,
            ),
            ModelRouteMemberRecord(
                route_id="agent.default_chat",
                model_id="openai-main/gpt-backup",
                priority=20,
                weight=1.0,
            ),
        ],
    )
    db.model_registry.upsert_route(
        ModelRouteRecord(id="plugin.embedding", purpose="embedding", strategy="priority"),
        members=[
            ModelRouteMemberRecord(
                route_id="plugin.embedding",
                model_id="openai-main/text-embedding",
                priority=10,
            )
        ],
    )


def test_build_litellm_kwargs_drops_empty_thinking_param():
    kwargs = build_litellm_kwargs(
        provider={
            "type": "openrouter",
            "base_url": "https://openrouter.ai/api/v1",
            "auth": {"api_key": "secret-key"},
            "default_params": {"thinking": {}, "temperature": 0.2},
        },
        model={
            "backend_model": "openrouter/nvidia/nemotron-3-super-120b-a12b:free",
            "default_params": {"max_tokens": 128},
        },
        call=ModelRuntimeCall(
            model_id="op-test/nemotron",
            caller="agent.runtime",
            messages=[{"role": "user", "content": "Hello"}],
        ),
        timeout_override=None,
    )

    assert "thinking" not in kwargs
    assert kwargs["temperature"] == 0.2
    assert kwargs["max_tokens"] == 128


def test_build_litellm_kwargs_normalizes_late_system_messages():
    kwargs = build_litellm_kwargs(
        provider={
            "type": "dashscope",
            "base_url": "https://dashscope-intl.aliyuncs.com/compatible-mode/v1",
            "auth": {"api_key": "secret-key"},
            "default_params": {},
        },
        model={
            "backend_model": "qwen3.5-plus",
            "default_params": {},
        },
        call=ModelRuntimeCall(
            model_id="dashscope/qwen3.5-plus",
            caller="agent.runtime",
            messages=[
                {"role": "system", "content": "base rules"},
                {"role": "user", "content": "first turn"},
                {"role": "system", "content": "new messages arrived"},
            ],
        ),
        timeout_override=None,
    )

    assert [message["role"] for message in kwargs["messages"]] == ["system", "user", "user"]
    assert kwargs["messages"][2]["content"] == "new messages arrived"


def test_build_litellm_kwargs_stringifies_uncached_dashscope_system_blocks():
    kwargs = build_litellm_kwargs(
        provider={
            "type": "dashscope",
            "base_url": "https://dashscope-intl.aliyuncs.com/compatible-mode/v1",
            "auth": {"api_key": "secret-key"},
            "default_params": {},
        },
        model={
            "backend_model": "qwen3.6-flash-2026-04-16",
            "default_params": {},
        },
        call=ModelRuntimeCall(
            model_id="dashscope/qwen3.6-flash",
            caller="media.sticker_summary_runner",
            messages=[
                {
                    "role": "system",
                    "content": [
                        {"type": "text", "text": "You are ShinBot's sticker summary agent."},
                    ],
                },
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "请理解这张图。"},
                        {"type": "image_url", "image_url": {"url": "data:image/gif;base64,abc"}},
                    ],
                },
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "media_inspection_result",
                    "schema": {"type": "object"},
                },
            },
        ),
        timeout_override=None,
    )

    assert kwargs["messages"][0]["content"] == "You are ShinBot's sticker summary agent."
    assert isinstance(kwargs["messages"][1]["content"], list)
    assert kwargs["response_format"]["type"] == "json_schema"


def test_build_litellm_kwargs_preserves_dashscope_cached_system_blocks():
    kwargs = build_litellm_kwargs(
        provider={
            "type": "dashscope",
            "base_url": "https://dashscope-intl.aliyuncs.com/compatible-mode/v1",
            "auth": {"api_key": "secret-key"},
            "default_params": {},
        },
        model={
            "backend_model": "qwen3.5-plus",
            "default_params": {},
        },
        call=ModelRuntimeCall(
            model_id="dashscope/qwen3.5-plus",
            caller="agent.runtime",
            messages=[
                {
                    "role": "system",
                    "content": [
                        {
                            "type": "text",
                            "text": "Long cached system prompt",
                            "cache_control": {"type": "ephemeral"},
                        },
                    ],
                },
                {"role": "user", "content": "Hello"},
            ],
        ),
        timeout_override=None,
    )

    assert isinstance(kwargs["messages"][0]["content"], list)
    assert kwargs["messages"][0]["content"][0]["cache_control"] == {"type": "ephemeral"}


def test_build_litellm_kwargs_maps_request_headers_to_extra_headers():
    kwargs = build_litellm_kwargs(
        provider={
            "type": "custom_openai",
            "base_url": "https://example.test/v1",
            "auth": {"api_key": "secret-key"},
            "default_params": {
                "requestHeaders": {"X-Provider": "provider"},
                "extra_headers": {"X-Existing": "yes"},
            },
        },
        model={
            "backend_model": "openai/custom",
            "default_params": {"requestHeaders": {"X-Model": "model"}},
        },
        call=ModelRuntimeCall(
            model_id="custom/model",
            caller="agent.runtime",
            messages=[{"role": "user", "content": "Hello"}],
            params={"requestHeaders": {"X-Call": "call"}},
        ),
        timeout_override=None,
    )

    assert "requestHeaders" not in kwargs
    assert kwargs["extra_headers"] == {
        "X-Existing": "yes",
        "X-Call": "call",
    }


def test_build_litellm_kwargs_allows_tools_and_response_format_for_strict_litellm():
    kwargs = build_litellm_kwargs(
        provider={
            "type": "xiaomi_mimo",
            "base_url": "https://token-plan-cn.xiaomimimo.com/v1",
            "auth": {"api_key": "secret-key"},
            "default_params": {"allowed_openai_params": ("temperature",)},
        },
        model={
            "backend_model": "xiaomi_mimo/mimo-v2.5",
            "default_params": {},
        },
        call=ModelRuntimeCall(
            model_id="mimo/mimo-v2.5",
            caller="agent.runtime",
            messages=[{"role": "user", "content": "Hello"}],
            tools=[{"type": "function", "function": {"name": "send_reply"}}],
            response_format={"type": "json_object"},
        ),
        timeout_override=None,
    )

    assert kwargs["allowed_openai_params"] == [
        "temperature",
        "tools",
        "tool_choice",
        "response_format",
    ]
    assert kwargs["custom_llm_provider"] == "openai"
    assert kwargs["model"] == "mimo-v2.5"
    assert kwargs["tools"] == [{"type": "function", "function": {"name": "send_reply"}}]


@pytest.mark.asyncio
async def test_generate_merges_provider_model_and_call_params(monkeypatch, tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    _seed_runtime(db)
    runtime = ModelRuntime(db)
    captured: dict[str, object] = {}

    def fake_completion(**kwargs):
        captured.update(kwargs)
        return {
            "model": kwargs["model"],
            "choices": [{"message": {"content": "hello from model"}}],
            "usage": {
                "prompt_tokens": 11,
                "completion_tokens": 22,
                "prompt_tokens_details": {
                    "cached_tokens": 3,
                    "cache_creation_input_tokens": 4,
                },
            },
            "_hidden_params": {"response_cost": 0.12},
        }

    monkeypatch.setattr("shinbot.agent.services.model_runtime.litellm_adapter.completion", fake_completion)

    result = await runtime.generate(
        ModelRuntimeCall(
            route_id="agent.default_chat",
            caller="agent.runtime",
            session_id="inst1:group:g1",
            instance_id="inst1",
            purpose="chat",
            messages=[{"role": "user", "content": "Hello"}],
            params={"temperature": 0.7},
            metadata={"trace_id": "trace-1"},
        )
    )

    assert result.text == "hello from model"
    assert captured["model"] == "openai/gpt-4.1-mini"
    assert captured["api_key"] == "secret-key"
    assert captured["api_base"] == "https://api.openai.com/v1"
    assert captured["temperature"] == 0.7
    assert captured["max_tokens"] == 128

    records = db.model_executions.list_recent(limit=1)
    assert records[0]["session_id"] == "inst1:group:g1"
    assert records[0]["input_tokens"] == 11
    assert records[0]["cache_hit"] is True
    assert records[0]["cache_read_tokens"] == 3
    assert records[0]["cache_write_tokens"] == 4
    assert records[0]["metadata"]["trace_id"] == "trace-1"


@pytest.mark.asyncio
async def test_default_backend_delegates_to_litellm_adapter_after_construction(
    monkeypatch,
    tmp_path,
):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    _seed_runtime(db)
    runtime = ModelRuntime(db)
    captured: dict[str, object] = {}

    def fake_completion(**kwargs):
        captured.update(kwargs)
        return {
            "model": kwargs["model"],
            "choices": [{"message": {"content": "patched after construction"}}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 2},
        }

    monkeypatch.setattr("shinbot.agent.services.model_runtime.litellm_adapter.completion", fake_completion)

    result = await runtime.generate(
        ModelRuntimeCall(
            route_id="agent.default_chat",
            caller="agent.runtime",
            messages=[{"role": "user", "content": "Hello"}],
        )
    )

    assert result.text == "patched after construction"
    assert captured["model"] == "openai/gpt-4.1-mini"
    assert captured["api_key"] == "secret-key"


@pytest.mark.asyncio
async def test_generate_uses_injected_backend(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    _seed_runtime(db)
    backend = RecordingBackend()
    runtime = ModelRuntime(db, backend=backend)

    result = await runtime.generate(
        ModelRuntimeCall(
            route_id="agent.default_chat",
            caller="agent.runtime",
            messages=[{"role": "user", "content": "Hello"}],
            params={"temperature": 0.7},
        )
    )

    assert result.text == "hello from backend"
    assert result.provider_id == "openai-main"
    assert result.model_id == "openai-main/gpt-fast"
    assert result.usage["input_tokens"] == 2
    assert backend.plans[0].backend_name == "recording"
    assert backend.plans[0].payload["temperature"] == 0.7

    record = db.model_executions.list_recent(limit=1)[0]
    assert record["success"] is True
    assert record["input_tokens"] == 2
    assert record["output_tokens"] == 3


@pytest.mark.asyncio
async def test_generate_persists_sanitized_image_context(monkeypatch, tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    _seed_runtime(db)
    runtime = ModelRuntime(db)
    captured: dict[str, object] = {}
    raw = b"image bytes that should not be stored in ai_interactions"
    encoded = base64.b64encode(raw).decode("ascii")
    digest = hashlib.sha256(raw).hexdigest()

    def fake_completion(**kwargs):
        captured.update(kwargs)
        return {
            "model": kwargs["model"],
            "choices": [{"message": {"content": "ok"}}],
            "usage": {"prompt_tokens": 3, "completion_tokens": 4},
        }

    monkeypatch.setattr("shinbot.agent.services.model_runtime.litellm_adapter.completion", fake_completion)

    result = await runtime.generate(
        ModelRuntimeCall(
            route_id="agent.default_chat",
            caller="media.inspection_runner",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "describe this image"},
                        {
                            "type": "image_url",
                            "image_url": {"url": f"data:image/png;base64,{encoded}"},
                        },
                    ],
                }
            ],
        )
    )

    sent_content = captured["messages"][0]["content"]
    assert sent_content[1]["image_url"]["url"] == f"data:image/png;base64,{encoded}"

    interaction = db.ai_interactions.get_by_execution(result.execution_id)
    assert interaction is not None
    assert encoded not in interaction["injected_context_json"]
    persisted_content = json.loads(interaction["injected_context_json"])
    assert persisted_content[1]["image_url"]["url"] == f"media:sha256:{digest}"
    assert persisted_content[1]["image_url"]["raw_hash"] == digest
    assert persisted_content[1]["image_url"]["redacted"] is True


@pytest.mark.asyncio
async def test_generate_falls_back_to_second_route_member(monkeypatch, tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    _seed_runtime(db)
    runtime = ModelRuntime(db)
    calls: list[str] = []

    def fake_completion(**kwargs):
        calls.append(kwargs["model"])
        if kwargs["model"] == "openai/gpt-4.1-mini":
            raise RuntimeError("upstream temporary failure")
        return {
            "model": kwargs["model"],
            "choices": [{"message": {"content": "fallback success"}}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 7},
        }

    monkeypatch.setattr("shinbot.agent.services.model_runtime.litellm_adapter.completion", fake_completion)

    result = await runtime.generate(
        ModelRuntimeCall(
            route_id="agent.default_chat",
            caller="agent.runtime",
            messages=[{"role": "user", "content": "hi"}],
            tools=[
                {
                    "type": "function",
                    "function": {
                        "name": "workflow.send_reply",
                        "description": "Send a reply",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "text": {"type": "string"},
                            },
                            "required": ["text"],
                        },
                    },
                }
            ],
        )
    )

    assert result.model_id == "openai-main/gpt-backup"
    assert calls == ["openai/gpt-4.1-mini", "openai/gpt-4.1"]

    records = db.model_executions.list_recent(limit=5)
    assert len(records) == 2
    success_record = records[0]
    failure_record = records[1]
    assert success_record["success"] is True
    assert failure_record["success"] is False
    assert success_record["fallback_from_model_id"] == "openai-main/gpt-fast"


@pytest.mark.asyncio
async def test_generate_returns_tool_calls(monkeypatch, tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    _seed_runtime(db)
    runtime = ModelRuntime(db)
    captured: dict[str, object] = {}

    def fake_completion(**kwargs):
        captured.update(kwargs)
        return {
            "model": kwargs["model"],
            "choices": [
                {
                    "message": {
                        "content": "",
                        "tool_calls": [
                            {
                                "id": "call_1",
                                "type": "function",
                                "function": {
                                    "name": "workflow.send_reply",
                                    "arguments": '{"text":"pong"}',
                                },
                            }
                        ],
                    }
                }
            ],
            "usage": {"prompt_tokens": 3, "completion_tokens": 4},
        }

    monkeypatch.setattr("shinbot.agent.services.model_runtime.litellm_adapter.completion", fake_completion)

    result = await runtime.generate(
        ModelRuntimeCall(
            route_id="agent.default_chat",
            caller="agent.runtime",
            messages=[{"role": "user", "content": "hi"}],
            tools=[
                {
                    "type": "function",
                    "function": {
                        "name": "workflow.send_reply",
                        "description": "Send a reply",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "text": {"type": "string"},
                            },
                            "required": ["text"],
                        },
                    },
                }
            ],
        )
    )

    assert result.tool_calls == [
        {
            "id": "call_1",
            "type": "function",
            "function": {
                "name": "workflow.send_reply",
                "arguments": '{"text":"pong"}',
            },
        }
    ]
    assert "tool_choice" not in captured


@pytest.mark.asyncio
async def test_embed_records_usage(monkeypatch, tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    _seed_runtime(db)
    runtime = ModelRuntime(db)

    def fake_embedding(**kwargs):
        assert kwargs["model"] == "openai/text-embedding-3-small"
        return {
            "data": [{"embedding": [0.1, 0.2, 0.3]}],
            "usage": {"prompt_tokens": 9},
        }

    monkeypatch.setattr("shinbot.agent.services.model_runtime.litellm_adapter.embedding", fake_embedding)

    result = await runtime.embed(
        ModelRuntimeCall(
            route_id="plugin.embedding",
            caller="plugin.summary",
            input_data="hello world",
        )
    )

    assert result.embedding == [0.1, 0.2, 0.3]
    records = db.model_executions.list_recent(limit=1)
    assert records[0]["model_id"] == "openai-main/text-embedding"
    assert records[0]["input_tokens"] == 9


@pytest.mark.asyncio
async def test_generate_passes_custom_llm_provider_for_custom_openai(monkeypatch, tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    db.model_registry.upsert_provider(
        ModelProviderRecord(
            id="custom-openai-main",
            type="custom_openai",
            display_name="Custom OpenAI Main",
            base_url="https://api.example.com/v1",
            auth={"api_key": "secret-key"},
        )
    )
    db.model_registry.upsert_model(
        ModelDefinitionRecord(
            id="custom-openai-main/qwen",
            provider_id="custom-openai-main",
            backend_model="qwen3.5-plus-2026-02-15",
            display_name="Qwen",
            capabilities=["chat"],
        )
    )
    runtime = ModelRuntime(db)
    captured: dict[str, object] = {}

    def fake_completion(**kwargs):
        captured.update(kwargs)
        return {
            "model": kwargs["model"],
            "choices": [{"message": {"content": "hello from custom provider"}}],
            "usage": {"prompt_tokens": 4, "completion_tokens": 6},
        }

    monkeypatch.setattr("shinbot.agent.services.model_runtime.litellm_adapter.completion", fake_completion)

    result = await runtime.generate(
        ModelRuntimeCall(
            model_id="custom-openai-main/qwen",
            caller="agent.runtime",
            messages=[{"role": "user", "content": "Hello"}],
        )
    )

    assert result.text == "hello from custom provider"
    assert captured["model"] == "qwen3.5-plus-2026-02-15"
    assert captured["api_base"] == "https://api.example.com/v1"
    assert captured["custom_llm_provider"] == "openai"


@pytest.mark.asyncio
async def test_generate_persists_audit_payload_to_file(monkeypatch, tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    _seed_runtime(db)
    runtime = ModelRuntime(db)

    def fake_completion(**kwargs):
        assert kwargs["messages"][0]["content"] == "Hello"
        return {
            "model": kwargs["model"],
            "choices": [{"message": {"content": "hello from model"}}],
            "usage": {"prompt_tokens": 7, "completion_tokens": 9},
            "_hidden_params": {"response_cost": 0.03},
        }

    monkeypatch.setattr("shinbot.agent.services.model_runtime.litellm_adapter.completion", fake_completion)

    result = await runtime.generate(
        ModelRuntimeCall(
            route_id="agent.default_chat",
            caller="agent.runtime",
            session_id="inst1:group:g1",
            instance_id="inst1",
            purpose="chat",
            messages=[{"role": "user", "content": "Hello"}],
            metadata={"trace_id": "trace-1"},
            params={"temperature": 0.7},
        )
    )

    record = db.model_executions.list_recent(limit=1)[0]
    assert record["metadata"]["audit_payload_ref"] == f"model-audit/{result.execution_id}.json"
    assert "audit_payload_expires_at" in record["metadata"]

    audit_path = tmp_path / "model-audit" / f"{result.execution_id}.json"
    payload = json.loads(audit_path.read_text(encoding="utf-8"))
    assert payload["execution_id"] == result.execution_id
    assert payload["request"]["caller"] == "agent.runtime"
    assert payload["request"]["messages"][0]["content"] == "Hello"
    assert payload["response"]["choices"][0]["message"]["content"] == "hello from model"
    assert payload["meta"]["operation"] == "generate"
    assert payload["status"] == "success"


@pytest.mark.asyncio
async def test_generate_requires_valid_target(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    runtime = ModelRuntime(db)

    with pytest.raises(ModelCallError):
        await runtime.generate(
            ModelRuntimeCall(
                route_id="missing.route",
                caller="agent.runtime",
                messages=[{"role": "user", "content": "hi"}],
            )
        )
