from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from shinbot.api.app import create_api_app
from shinbot.core.application.app import ShinBot
from shinbot.persistence import ModelExecutionRecord

pytestmark = [pytest.mark.api, pytest.mark.slow]


def test_cost_analysis_endpoint_allows_null_latency_metrics(
    tmp_path: Path,
    make_boot_stub,
    make_auth_headers,
):
    bot = ShinBot(data_dir=tmp_path)
    bot.database.model_executions.insert(
        ModelExecutionRecord(
            id="exec-null-latency",
            started_at=datetime.now(UTC).isoformat(),
            provider_id="openai-main",
            model_id="openai-main/gpt-test",
            success=True,
            input_tokens=10,
            output_tokens=20,
        )
    )
    app = create_api_app(bot, make_boot_stub(tmp_path))
    headers = make_auth_headers(app)

    with TestClient(app) as client:
        response = client.get("/api/v1/model-runtime/cost-analysis", headers=headers)

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["summary"]["averageLatencyMs"] is None
    assert payload["summary"]["averageTimeToFirstTokenMs"] is None
    assert payload["models"][0]["averageLatencyMs"] is None
    assert payload["models"][0]["averageTimeToFirstTokenMs"] is None
