from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from shinbot.api.app import create_api_app
from shinbot.api.routers.model_runtime import get_cost_analysis
from shinbot.core.application.app import ShinBot
from shinbot.persistence import ModelDefinitionRecord, ModelExecutionRecord, ModelProviderRecord

pytestmark = [pytest.mark.integration, pytest.mark.slow]


def test_model_execution_list_endpoint(tmp_path: Path, make_boot_stub, make_auth_headers):
    bot = ShinBot(data_dir=tmp_path)
    bot.database.model_executions.insert(
        ModelExecutionRecord(
            id="exec-1",
            route_id="agent.default_chat",
            provider_id="openai-main",
            model_id="openai-main/gpt-4.1-mini",
            caller="agent.runtime",
            session_id="inst1:group:g1",
            instance_id="inst1",
            success=True,
            input_tokens=12,
            output_tokens=34,
            cache_hit=True,
            metadata={"trace_id": "trace-1"},
        )
    )
    app = create_api_app(bot, make_boot_stub(tmp_path))
    headers = make_auth_headers(app)

    with TestClient(app) as client:
        response = client.get("/api/v1/model-runtime/executions", headers=headers)

    assert response.status_code == 200
    payload = response.json()["data"][0]
    assert payload["id"] == "exec-1"
    assert payload["cacheHit"] is True
    assert payload["metadata"]["trace_id"] == "trace-1"


def test_model_token_summary_endpoint(tmp_path: Path, make_boot_stub, make_auth_headers):
    bot = ShinBot(data_dir=tmp_path)
    recent_started_at = (datetime.now(UTC) - timedelta(days=1)).isoformat()
    old_started_at = (datetime.now(UTC) - timedelta(days=30)).isoformat()
    bot.database.model_executions.insert(
        ModelExecutionRecord(
            id="exec-recent-1",
            started_at=recent_started_at,
            provider_id="openai-main",
            model_id="openai-main/gpt-fast",
            success=True,
            input_tokens=10,
            output_tokens=20,
            cache_read_tokens=3,
            cache_write_tokens=2,
            estimated_cost=0.12,
        )
    )
    bot.database.model_executions.insert(
        ModelExecutionRecord(
            id="exec-recent-2",
            started_at=recent_started_at,
            provider_id="openai-main",
            model_id="openai-main/gpt-fast",
            success=False,
            input_tokens=5,
            output_tokens=0,
        )
    )
    bot.database.model_executions.insert(
        ModelExecutionRecord(
            id="exec-old",
            started_at=old_started_at,
            provider_id="openai-main",
            model_id="openai-main/gpt-old",
            success=True,
            input_tokens=1000,
            output_tokens=1000,
        )
    )
    app = create_api_app(bot, make_boot_stub(tmp_path))
    headers = make_auth_headers(app)

    with TestClient(app) as client:
        response = client.get(
            "/api/v1/model-runtime/token-summary",
            params={"days": 7},
            headers=headers,
        )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["windowDays"] == 7
    assert payload["totalCalls"] == 2
    assert payload["successfulCalls"] == 1
    assert payload["inputTokens"] == 15
    assert payload["outputTokens"] == 20
    assert payload["totalTokens"] == 35
    assert payload["cacheReadTokens"] == 3
    assert payload["cacheWriteTokens"] == 2
    assert payload["topModels"][0]["modelId"] == "openai-main/gpt-fast"
    assert payload["topModels"][0]["totalTokens"] == 35


@pytest.mark.asyncio
async def test_cost_analysis_endpoint_returns_timeline_and_focus_models(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    bot.database.model_registry.upsert_provider(
        ModelProviderRecord(
            id="openai-main",
            type="openai",
            display_name="OpenAI Main",
        )
    )
    bot.database.model_registry.upsert_provider(
        ModelProviderRecord(
            id="anthropic-main",
            type="anthropic",
            display_name="Anthropic Main",
        )
    )
    bot.database.model_registry.upsert_model(
        ModelDefinitionRecord(
            id="openai-main/gpt-fast",
            provider_id="openai-main",
            litellm_model="openai/gpt-4.1-mini",
            display_name="GPT Fast",
            cost_metadata={
                "inputPer1kTokens": 1.0,
                "outputPer1kTokens": 2.0,
                "cacheReadPer1kTokens": 0.5,
                "cacheWritePer1kTokens": 1.0,
            },
        )
    )
    bot.database.model_registry.upsert_model(
        ModelDefinitionRecord(
            id="anthropic-main/claude",
            provider_id="anthropic-main",
            litellm_model="anthropic/claude-sonnet-4",
            display_name="Claude",
            cost_metadata={
                "inputPer1kTokens": 2.0,
                "outputPer1kTokens": 0.5,
                "cacheWritePer1kTokens": 1.0,
            },
        )
    )

    now = datetime.now(UTC)
    current_bucket_time = now.replace(minute=15, second=0, microsecond=0)
    previous_day_time = current_bucket_time - timedelta(days=1)
    old_time = current_bucket_time - timedelta(days=10)

    bot.database.model_executions.insert(
        ModelExecutionRecord(
            id="exec-cost-1",
            started_at=current_bucket_time.isoformat(),
            provider_id="openai-main",
            model_id="openai-main/gpt-fast",
            success=True,
            input_tokens=120,
            output_tokens=60,
            cache_hit=True,
            cache_read_tokens=40,
            cache_write_tokens=12,
            latency_ms=860,
            time_to_first_token_ms=210,
        )
    )
    bot.database.model_executions.insert(
        ModelExecutionRecord(
            id="exec-cost-2",
            started_at=previous_day_time.isoformat(),
            provider_id="openai-main",
            model_id="openai-main/gpt-fast",
            success=False,
            input_tokens=80,
            output_tokens=0,
            latency_ms=1240,
        )
    )
    bot.database.model_executions.insert(
        ModelExecutionRecord(
            id="exec-cost-3",
            started_at=current_bucket_time.isoformat(),
            provider_id="anthropic-main",
            model_id="anthropic-main/claude",
            success=True,
            input_tokens=40,
            output_tokens=120,
            cache_write_tokens=20,
            latency_ms=640,
            time_to_first_token_ms=180,
        )
    )
    bot.database.model_executions.insert(
        ModelExecutionRecord(
            id="exec-cost-old",
            started_at=old_time.isoformat(),
            provider_id="openai-main",
            model_id="openai-main/gpt-fast",
            success=True,
            input_tokens=999,
            output_tokens=999,
        )
    )

    response = await get_cost_analysis(days=2, modelLimit=1, bot=bot)
    payload = response["data"]
    assert payload["windowDays"] == 2
    assert payload["summary"]["totalCalls"] == 3
    assert payload["summary"]["successfulCalls"] == 2
    assert payload["summary"]["failedCalls"] == 1
    assert payload["summary"]["cacheHits"] == 1
    assert payload["summary"]["cacheHitRate"] == pytest.approx(1 / 3)
    assert payload["summary"]["inputTokens"] == 240
    assert payload["summary"]["outputTokens"] == 180
    assert payload["summary"]["totalTokens"] == 420
    assert payload["summary"]["cacheReadTokens"] == 40
    assert payload["summary"]["cacheWriteTokens"] == 32
    assert payload["summary"]["estimatedCost"] == pytest.approx(0.42)

    assert len(payload["timeline"]["daily"]) == 2
    assert payload["timeline"]["daily"][0]["totalCalls"] == 1
    assert payload["timeline"]["daily"][1]["totalCalls"] == 2
    assert len(payload["timeline"]["hourly"]) == 24
    assert sum(item["totalCalls"] for item in payload["timeline"]["hourly"]) == 2

    assert len(payload["models"]) == 2
    assert payload["models"][0]["modelId"] == "openai-main/gpt-fast"
    assert payload["models"][0]["modelDisplayName"] == "GPT Fast"
    assert payload["models"][0]["estimatedCost"] == pytest.approx(0.3)
    assert payload["models"][0]["cacheHitRate"] == pytest.approx(0.5)

    assert len(payload["focusModels"]) == 1
    assert payload["focusModels"][0]["modelId"] == "openai-main/gpt-fast"
    assert len(payload["focusModels"][0]["daily"]) == 2
    assert len(payload["focusModels"][0]["hourly"]) == 24


@pytest.mark.asyncio
async def test_cost_estimation_does_not_double_charge_cached_input(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    bot.database.model_registry.upsert_provider(
        ModelProviderRecord(
            id="openai-main",
            type="openai",
            display_name="OpenAI Main",
        )
    )
    bot.database.model_registry.upsert_model(
        ModelDefinitionRecord(
            id="openai-main/gpt-cached",
            provider_id="openai-main",
            litellm_model="openai/gpt-4.1-mini",
            display_name="GPT Cached",
            cost_metadata={
                "inputPer1kTokens": 1.0,
                "outputPer1kTokens": 2.0,
                "cacheReadPer1kTokens": 0.1,
                "cacheWritePer1kTokens": 0.5,
            },
        )
    )
    bot.database.model_executions.insert(
        ModelExecutionRecord(
            id="exec-cached-cost",
            started_at=datetime.now(UTC).isoformat(),
            provider_id="openai-main",
            model_id="openai-main/gpt-cached",
            success=True,
            input_tokens=100,
            output_tokens=50,
            cache_read_tokens=30,
            cache_write_tokens=20,
        )
    )

    response = await get_cost_analysis(days=7, modelLimit=1, bot=bot)
    payload = response["data"]
    assert payload["summary"]["estimatedCost"] == pytest.approx(0.163)


