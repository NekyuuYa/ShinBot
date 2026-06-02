from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from shinbot.api.app import create_api_app
from shinbot.core.application.app import ShinBot


def test_model_runtime_metadata_endpoints_expose_backends_and_provider_types(
    tmp_path: Path,
    make_boot_stub,
    make_auth_headers,
):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, make_boot_stub(tmp_path))
    headers = make_auth_headers(app)

    with TestClient(app) as client:
      backends = client.get("/api/v1/model-runtime/metadata/backends", headers=headers)
      provider_types = client.get(
          "/api/v1/model-runtime/metadata/provider-types",
          headers=headers,
      )

    assert backends.status_code == 200
    backend_items = backends.json()["data"]
    backend_names = {item["name"] for item in backend_items}
    assert "litellm" in backend_names
    assert "openai_compatible" in backend_names
    openai_backend = next(item for item in backend_items if item["name"] == "openai_compatible")
    assert "openai" in openai_backend["supportedProviderTypes"]
    assert "custom_openai" in openai_backend["supportedProviderTypes"]

    assert provider_types.status_code == 200
    provider_type_items = provider_types.json()["data"]
    openai = next(item for item in provider_type_items if item["type"] == "openai")
    assert openai["displayName"] == "OpenAI"
    assert openai["supportsCatalog"] is True
    assert openai["defaultBaseUrl"] == "https://api.openai.com/v1"
    assert openai["presets"][0]["key"] == "openai"
    auth_fields = [field for field in openai["configFields"] if field["location"] == "auth"]
    assert auth_fields[0]["key"] == "api_key"
    assert auth_fields[0]["control"] == "secret"

    ollama = next(item for item in provider_type_items if item["type"] == "ollama")
    assert ollama["authStrategy"] == "bearer"
    assert ollama["authParamKey"] == "api_key"
    assert ollama["supportsCatalog"] is True
    assert not any(field["location"] == "auth" for field in ollama["configFields"])
