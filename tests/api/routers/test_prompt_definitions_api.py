from __future__ import annotations

from fastapi.testclient import TestClient


def test_prompt_definition_crud_roundtrip(router_api):
    tmp_path = router_api.data_dir

    with TestClient(router_api.app) as client:
        create_resp = client.post(
            "/api/v1/prompt-definitions",
            headers=router_api.headers,
            json={
                "promptId": "prompt.identity.extra",
                "name": "Identity Extra",
                "sourceType": "agent_plugin",
                "sourceId": "plugin.identity",
                "ownerPluginId": "plugin.identity",
                "ownerModule": "shinbot.plugins.identity",
                "stage": "identity",
                "type": "static_text",
                "priority": 20,
                "description": "Additional identity prompt",
                "content": "You are calm and concise.",
                "tags": ["identity", "identity", "agent"],
                "metadata": {"display_name": "Identity Extra"},
            },
        )
        assert create_resp.status_code == 201
        created = create_resp.json()["data"]
        assert created["uuid"]
        assert created["promptId"] == "prompt.identity.extra"
        assert created["source"]["sourceType"] == "agent_plugin"
        assert created["source"]["sourceId"] == "plugin.identity"
        assert created["tags"] == ["identity", "agent"]
        assert created["metadata"] == {}
        assert (tmp_path / "prompts" / "custom" / "prompt.identity.extra.md").is_file()

        prompt_uuid = created["uuid"]

        get_resp = client.get(
            f"/api/v1/prompt-definitions/{prompt_uuid}",
            headers=router_api.headers,
        )
        assert get_resp.status_code == 200
        assert get_resp.json()["data"]["uuid"] == prompt_uuid

        patch_resp = client.patch(
            f"/api/v1/prompt-definitions/{prompt_uuid}",
            headers=router_api.headers,
            json={
                "promptId": "prompt.instructions.chat",
                "name": "Chat Instructions",
                "sourceType": "builtin_system",
                "sourceId": "builtin.chat",
                "stage": "instructions",
                "type": "template",
                "content": "task={task}",
                "templateVars": ["task"],
            },
        )
        assert patch_resp.status_code == 200
        patched = patch_resp.json()["data"]
        assert patched["promptId"] == "prompt.instructions.chat"
        assert patched["uuid"] == "prompt.instructions.chat"
        assert patched["source"]["sourceType"] == "builtin_system"
        assert patched["source"]["sourceId"] == "builtin.chat"
        assert patched["type"] == "template"
        assert patched["templateVars"] == ["task"]
        assert not (tmp_path / "prompts" / "custom" / "prompt.identity.extra.md").exists()
        assert (tmp_path / "prompts" / "custom" / "prompt.instructions.chat.md").is_file()

        list_resp = client.get("/api/v1/prompt-definitions", headers=router_api.headers)
        assert list_resp.status_code == 200
        assert len(list_resp.json()["data"]) == 1
        assert list_resp.json()["data"][0]["uuid"] == "prompt.instructions.chat"

        delete_resp = client.delete(
            "/api/v1/prompt-definitions/prompt.instructions.chat",
            headers=router_api.headers,
        )
        assert delete_resp.status_code == 200
        assert delete_resp.json()["data"]["deleted"] is True


def test_prompt_definition_rejects_duplicate_id_and_invalid_shape(router_api):
    with TestClient(router_api.app) as client:
        first_resp = client.post(
            "/api/v1/prompt-definitions",
            headers=router_api.headers,
            json={
                "promptId": "prompt.identity.extra",
                "name": "Identity Extra",
                "stage": "identity",
                "type": "static_text",
                "content": "hello",
            },
        )
        assert first_resp.status_code == 201

        duplicate_resp = client.post(
            "/api/v1/prompt-definitions",
            headers=router_api.headers,
            json={
                "promptId": "prompt.identity.extra",
                "name": "Identity Extra 2",
                "stage": "identity",
                "type": "static_text",
                "content": "hello again",
            },
        )
        assert duplicate_resp.status_code == 409
        assert duplicate_resp.json()["error"]["code"] == "PROMPT_ALREADY_EXISTS"

        invalid_resp = client.post(
            "/api/v1/prompt-definitions",
            headers=router_api.headers,
            json={
                "promptId": "prompt.instructions.bad",
                "name": "Bad Template",
                "stage": "instructions",
                "type": "template",
                "content": "task={task}",
            },
        )
        assert invalid_resp.status_code == 400
        assert invalid_resp.json()["error"]["code"] == "INVALID_ACTION"

        persona_source_resp = client.post(
            "/api/v1/prompt-definitions",
            headers=router_api.headers,
            json={
                "promptId": "persona.legacy",
                "name": "Legacy Persona Prompt",
                "sourceType": "persona",
                "stage": "identity",
                "type": "static_text",
                "content": "Use data/personas instead.",
            },
        )
        assert persona_source_resp.status_code == 400
        assert persona_source_resp.json()["error"]["code"] == "INVALID_ACTION"


def test_prompt_definition_create_and_rename_reject_runtime_prompt_conflict(router_api):
    with TestClient(router_api.app) as client:
        conflict_create = client.post(
            "/api/v1/prompt-definitions",
            headers=router_api.headers,
            json={
                "promptId": "review.review_scan.task",
                "name": "Conflicting Prompt",
                "stage": "instructions",
                "type": "static_text",
                "content": "hello",
            },
        )
        assert conflict_create.status_code == 409
        assert conflict_create.json()["error"]["code"] == "PROMPT_FILE_CONFLICT"

        create_resp = client.post(
            "/api/v1/prompt-definitions",
            headers=router_api.headers,
            json={
                "promptId": "prompt.identity.extra",
                "name": "Identity Extra",
                "stage": "identity",
                "type": "static_text",
                "content": "hello",
            },
        )
        assert create_resp.status_code == 201

        conflict_patch = client.patch(
            "/api/v1/prompt-definitions/prompt.identity.extra",
            headers=router_api.headers,
            json={"promptId": "review.review_scan.task"},
        )
        assert conflict_patch.status_code == 409
        assert conflict_patch.json()["error"]["code"] == "PROMPT_FILE_CONFLICT"


def test_prompt_definition_runtime_conflict_uses_exact_registry_id(router_api):
    prompt_id = "review.review_scan.task.custom.extension.with.long.prompt.id"

    with TestClient(router_api.app) as client:
        create_resp = client.post(
            "/api/v1/prompt-definitions",
            headers=router_api.headers,
            json={
                "promptId": prompt_id,
                "name": "Long Custom Prompt",
                "stage": "instructions",
                "type": "static_text",
                "content": "hello",
            },
        )

    assert create_resp.status_code == 201
    assert create_resp.json()["data"]["promptId"] == prompt_id
