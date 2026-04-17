from __future__ import annotations

import json
from pathlib import Path

import pytest

from shinbot.agent.model_runtime import ModelRuntimeCall
from shinbot.builtin_plugins.shinbot_debug_model import _build_model_record
from shinbot.core.application.app import ShinBot
from shinbot.persistence import ModelDefinitionRecord, ModelProviderRecord


def test_build_model_record_preserves_request_payload():
    record = _build_model_record(
        {
            "event": "model_runtime.request",
            "mode": "completion",
            "caller": "agent.runtime",
            "purpose": "chat",
            "session_id": "inst:group:1",
            "instance_id": "inst",
            "route_id": "agent.default_chat",
            "provider_id": "openai-main",
            "provider_type": "openai",
            "model_id": "openai-main/gpt-fast",
            "litellm_model": "openai/gpt-4.1-mini",
            "execution_id": "exec-1",
            "strategy": "priority",
            "messages": [{"role": "user", "content": "hello"}],
            "params": {"max_tokens": 16},
            "kwargs": {"api_key": "***", "model": "openai/gpt-4.1-mini"},
            "metadata": {"trace_id": "trace-1"},
            "prompt_snapshot_id": "snap-1",
        }
    )

    assert record["event_type"] == "model_runtime.request"
    assert record["mode"] == "completion"
    assert record["model_id"] == "openai-main/gpt-fast"
    assert record["request"]["messages"] == [{"role": "user", "content": "hello"}]
    assert record["request"]["params"] == {"max_tokens": 16}
    assert record["request"]["kwargs"]["api_key"] == "***"


def test_build_model_record_preserves_response_payload():
    record = _build_model_record(
        {
            "event": "model_runtime.response",
            "mode": "completion",
            "caller": "agent.runtime",
            "provider_id": "openai-main",
            "model_id": "openai-main/gpt-fast",
            "execution_id": "exec-1",
            "text": "ok",
            "usage": {"input_tokens": 1, "output_tokens": 1},
            "raw_response": {"model": "openai/gpt-4.1-mini"},
        }
    )

    assert record["event_type"] == "model_runtime.response"
    assert record["response"]["text"] == "ok"
    assert record["response"]["usage"] == {"input_tokens": 1, "output_tokens": 1}


@pytest.mark.asyncio
async def test_debug_model_plugin_persists_runtime_requests(tmp_path, monkeypatch: pytest.MonkeyPatch):
    bot = ShinBot(data_dir=tmp_path)
    metadata_path = (
        Path(__file__).resolve().parents[1]
        / "shinbot/builtin_plugins/shinbot_debug_model/metadata.json"
    )
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    await bot.plugin_manager.load_plugin_async(
        "shinbot_debug_model",
        "shinbot.builtin_plugins.shinbot_debug_model",
        declared_metadata=metadata,
    )

    bot.database.model_registry.upsert_provider(
        ModelProviderRecord(
            id="openai-main",
            type="openai",
            display_name="OpenAI Main",
            auth={"api_key": "secret-key"},
        )
    )
    bot.database.model_registry.upsert_model(
        ModelDefinitionRecord(
            id="openai-main/gpt-fast",
            provider_id="openai-main",
            litellm_model="openai/gpt-4.1-mini",
            display_name="GPT Fast",
            capabilities=["chat"],
        )
    )

    def fake_completion(**kwargs):
        return {
            "choices": [{"message": {"content": "ok"}}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1},
        }

    monkeypatch.setattr("shinbot.agent.model_runtime.litellm_adapter.completion", fake_completion)

    await bot.model_runtime.generate(
        ModelRuntimeCall(
            model_id="openai-main/gpt-fast",
            caller="agent.runtime",
            purpose="chat",
            session_id="inst:group:1",
            instance_id="inst",
            messages=[{"role": "user", "content": "hello"}],
            params={"max_tokens": 8},
            metadata={"trace_id": "trace-1"},
        )
    )
    await bot.plugin_manager.unload_plugin_async("shinbot_debug_model")

    records_path = tmp_path / "plugin_data" / "shinbot_debug_model" / "model_requests.jsonl"
    lines = records_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
    request_payload = json.loads(lines[0])
    response_payload = json.loads(lines[1])
    assert request_payload["event_type"] == "model_runtime.request"
    assert request_payload["mode"] == "completion"
    assert request_payload["caller"] == "agent.runtime"
    assert request_payload["model_id"] == "openai-main/gpt-fast"
    assert request_payload["request"]["messages"] == [{"role": "user", "content": "hello"}]
    assert request_payload["request"]["kwargs"]["api_key"] == "***"
    assert response_payload["event_type"] == "model_runtime.response"
    assert response_payload["response"]["text"] == "ok"
    assert response_payload["response"]["usage"] == {
        "input_tokens": 1,
        "output_tokens": 1,
        "cache_read_tokens": 0,
        "cache_write_tokens": 0,
    }
