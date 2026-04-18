from __future__ import annotations

import pytest

from shinbot.agent.model_runtime import ModelCallError, ModelRuntimeCall
from shinbot.agent.model_runtime.service import ModelRuntime
from shinbot.persistence import DatabaseManager
from shinbot.persistence.records import (
    ModelDefinitionRecord,
    ModelProviderRecord,
    ModelRouteMemberRecord,
    ModelRouteRecord,
)


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
            litellm_model="openai/gpt-4.1-mini",
            display_name="GPT Fast",
            capabilities=["chat"],
            default_params={"max_tokens": 128},
        )
    )
    db.model_registry.upsert_model(
        ModelDefinitionRecord(
            id="openai-main/gpt-backup",
            provider_id="openai-main",
            litellm_model="openai/gpt-4.1",
            display_name="GPT Backup",
            capabilities=["chat"],
        )
    )
    db.model_registry.upsert_model(
        ModelDefinitionRecord(
            id="openai-main/text-embedding",
            provider_id="openai-main",
            litellm_model="openai/text-embedding-3-small",
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
                "prompt_tokens_details": {"cached_tokens": 3},
                "cache_creation_input_tokens": 4,
            },
            "_hidden_params": {"response_cost": 0.12},
        }

    monkeypatch.setattr("shinbot.agent.model_runtime.litellm_adapter.completion", fake_completion)

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
    assert records[0]["metadata"]["trace_id"] == "trace-1"


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

    monkeypatch.setattr("shinbot.agent.model_runtime.litellm_adapter.completion", fake_completion)

    result = await runtime.generate(
        ModelRuntimeCall(
            route_id="agent.default_chat",
            caller="agent.runtime",
            messages=[{"role": "user", "content": "hi"}],
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

    monkeypatch.setattr("shinbot.agent.model_runtime.litellm_adapter.embedding", fake_embedding)

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
            litellm_model="qwen3.5-plus-2026-02-15",
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

    monkeypatch.setattr("shinbot.agent.model_runtime.litellm_adapter.completion", fake_completion)

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
