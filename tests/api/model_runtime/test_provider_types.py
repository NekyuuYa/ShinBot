from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from shinbot.api.app import create_api_app
from shinbot.core.application.app import ShinBot


def test_create_provider_rejects_unknown_provider_type(
    tmp_path: Path,
    make_boot_stub,
    make_auth_headers,
):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, make_boot_stub(tmp_path))
    headers = make_auth_headers(app)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/model-runtime/providers",
            headers=headers,
            json={
                "id": "unknown-provider",
                "type": "definitely_unknown_provider",
                "displayName": "Unknown Provider",
                "baseUrl": "https://example.invalid",
            },
        )

    assert response.status_code == 400
    assert "Unknown model provider type" in response.json()["error"]["message"]
