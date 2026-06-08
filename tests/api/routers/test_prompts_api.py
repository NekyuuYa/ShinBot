from __future__ import annotations

from fastapi.testclient import TestClient

from shinbot.admin.prompt_definition_admin import (
    PromptDefinitionFileRepository,
    normalize_prompt_definition_input,
)


def test_prompts_list_discovers_runtime_files_without_agent_runtime(router_api):
    tmp_path = router_api.data_dir

    with TestClient(router_api.app) as client:
        response = client.get("/api/v1/prompts", headers=router_api.headers)

    assert response.status_code == 200
    payload = response.json()["data"]
    payload_by_id = {item["id"]: item for item in payload}
    assert "review.review_scan.task" in payload_by_id
    item = payload_by_id["review.review_scan.task"]
    assert item["fileId"] == "runtime~zh-CN~review.review_scan.task"
    assert item["layer"] == "runtime"
    assert item["locale"] == "zh-CN"
    assert item["editable"] is True
    assert item["deletable"] is False
    assert item["sourceStatus"] == "source"
    assert item["loadedFrom"] == "source"
    assert not (tmp_path / "prompts" / "zh-CN" / "review.review_scan.task.md").exists()


def test_prompts_list_includes_custom_prompt_definitions(router_api):
    tmp_path = router_api.data_dir
    PromptDefinitionFileRepository.from_data_dir(tmp_path).create(
        normalize_prompt_definition_input(
            prompt_id="prompt.user.custom",
            name="User Custom Prompt",
            stage="instructions",
            type="static_text",
            source_type="unknown_source",
            source_id="",
            owner_plugin_id="",
            owner_module="",
            module_path="",
            priority=55,
            version="1.0.0",
            description="Custom prompt from file",
            enabled=True,
            content="custom prompt text",
            template_vars=[],
            resolver_ref="",
            bundle_refs=[],
            config={},
            tags=[],
            metadata={"display_name": "User Custom Prompt"},
        )
    )
    with TestClient(router_api.app) as client:
        response = client.get("/api/v1/prompts", headers=router_api.headers)

    assert response.status_code == 200
    payload = response.json()["data"]
    payload_by_file_id = {item["fileId"]: item for item in payload}
    assert payload_by_file_id["custom~prompt.user.custom"] == {
        "id": "prompt.user.custom",
        "fileId": "custom~prompt.user.custom",
        "layer": "custom",
        "locale": "custom",
        "displayName": "User Custom Prompt",
        "description": "Custom prompt from file",
        "stage": "instructions",
        "type": "static_text",
        "version": "1.0.0",
        "priority": 55,
        "enabled": True,
        "resolverRef": "",
        "templateVars": [],
        "bundleRefs": [],
        "tags": [],
        "sourceType": "unknown_source",
        "sourceId": "",
        "ownerPluginId": "",
        "ownerModule": "",
        "modulePath": "",
        "editable": True,
        "deletable": True,
        "resettable": False,
        "sourceStatus": "custom",
        "loadedFrom": "custom",
        "sourcePath": "",
        "runtimePath": str(tmp_path / "prompts" / "custom" / "prompt.user.custom.md"),
        "loadedPath": str(tmp_path / "prompts" / "custom" / "prompt.user.custom.md"),
        "metadata": {},
    }


def test_runtime_prompt_get_patch_and_reset(router_api):
    tmp_path = router_api.data_dir
    file_id = "runtime~zh-CN~review.review_scan.task"
    runtime_path = tmp_path / "prompts" / "zh-CN" / "review.review_scan.task.md"

    with TestClient(router_api.app) as client:
        get_resp = client.get(f"/api/v1/prompts/{file_id}", headers=router_api.headers)
        assert get_resp.status_code == 200
        original = get_resp.json()["data"]
        assert original["promptId"] == "review.review_scan.task"
        assert "未读消息" in original["content"]
        assert not runtime_path.exists()

        patch_resp = client.patch(
            f"/api/v1/prompts/{file_id}",
            headers=router_api.headers,
            json={"content": "User edited runtime prompt."},
        )
        assert patch_resp.status_code == 200
        patched = patch_resp.json()["data"]
        assert patched["content"] == "User edited runtime prompt."

        assert "User edited runtime prompt." in runtime_path.read_text(encoding="utf-8")

        reset_resp = client.post(f"/api/v1/prompts/{file_id}/reset", headers=router_api.headers)
        assert reset_resp.status_code == 200
        assert reset_resp.json()["data"] == {"reset": True, "fileId": file_id}

        after_reset_resp = client.get(
            f"/api/v1/prompts/{file_id}",
            headers=router_api.headers,
        )
        assert after_reset_resp.status_code == 200
        assert "User edited runtime prompt." not in after_reset_resp.json()["data"]["content"]


def test_runtime_prompt_patch_rejects_structure_fields(router_api):
    tmp_path = router_api.data_dir
    file_id = "runtime~zh-CN~review.review_scan.task"
    runtime_path = tmp_path / "prompts" / "zh-CN" / "review.review_scan.task.md"

    with TestClient(router_api.app) as client:
        response = client.patch(
            f"/api/v1/prompts/{file_id}",
            headers=router_api.headers,
            json={"content": "User edited runtime prompt.", "stage": "identity"},
        )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "INVALID_ACTION"
    assert not runtime_path.exists()


def test_prompts_custom_create_get_patch_delete_and_runtime_delete_rejected(router_api):
    tmp_path = router_api.data_dir

    with TestClient(router_api.app) as client:
        create_resp = client.post(
            "/api/v1/prompts/custom",
            headers=router_api.headers,
            json={
                "promptId": "prompt.user.custom",
                "name": "User Custom Prompt",
                "stage": "instructions",
                "type": "static_text",
                "content": "hello",
            },
        )
        assert create_resp.status_code == 201
        file_id = create_resp.json()["data"]["fileId"]
        assert file_id == "custom~prompt.user.custom"

        get_resp = client.get(f"/api/v1/prompts/{file_id}", headers=router_api.headers)
        assert get_resp.status_code == 200
        assert get_resp.json()["data"]["content"] == "hello"

        patch_resp = client.patch(
            f"/api/v1/prompts/{file_id}",
            headers=router_api.headers,
            json={"content": "updated", "tags": ["user"]},
        )
        assert patch_resp.status_code == 200
        assert patch_resp.json()["data"]["content"] == "updated"
        assert patch_resp.json()["data"]["tags"] == ["user"]

        runtime_delete_resp = client.delete(
            "/api/v1/prompts/runtime~zh-CN~review.review_scan.task",
            headers=router_api.headers,
        )
        assert runtime_delete_resp.status_code == 400
        assert runtime_delete_resp.json()["error"]["code"] == "INVALID_ACTION"

        delete_resp = client.delete(f"/api/v1/prompts/{file_id}", headers=router_api.headers)
        assert delete_resp.status_code == 200
        assert delete_resp.json()["data"] == {"deleted": True, "fileId": file_id}
        assert not (tmp_path / "prompts" / "custom" / "prompt.user.custom.md").exists()


def test_prompts_custom_file_id_supports_colon_prompt_id(router_api):
    tmp_path = router_api.data_dir

    with TestClient(router_api.app) as client:
        create_resp = client.post(
            "/api/v1/prompts/custom",
            headers=router_api.headers,
            json={
                "promptId": "prompt:user.custom",
                "name": "Colon Custom Prompt",
                "stage": "instructions",
                "type": "static_text",
                "content": "hello",
            },
        )
        assert create_resp.status_code == 201
        file_id = create_resp.json()["data"]["fileId"]
        assert file_id == "custom~prompt%3Auser.custom"

        get_resp = client.get(f"/api/v1/prompts/{file_id}", headers=router_api.headers)
        assert get_resp.status_code == 200
        assert get_resp.json()["data"]["promptId"] == "prompt:user.custom"

        patch_resp = client.patch(
            f"/api/v1/prompts/{file_id}",
            headers=router_api.headers,
            json={"content": "updated"},
        )
        assert patch_resp.status_code == 200
        assert patch_resp.json()["data"]["content"] == "updated"

        delete_resp = client.delete(f"/api/v1/prompts/{file_id}", headers=router_api.headers)
        assert delete_resp.status_code == 200
        assert delete_resp.json()["data"] == {"deleted": True, "fileId": file_id}
        assert not (tmp_path / "prompts" / "custom" / "prompt:user.custom.md").exists()


def test_custom_prompt_create_and_rename_reject_runtime_prompt_conflict(router_api):
    headers = router_api.headers

    with TestClient(router_api.app) as client:
        conflict_create = client.post(
            "/api/v1/prompts/custom",
            headers=headers,
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
            "/api/v1/prompts/custom",
            headers=headers,
            json={
                "promptId": "prompt.user.custom",
                "name": "User Custom Prompt",
                "stage": "instructions",
                "type": "static_text",
                "content": "hello",
            },
        )
        assert create_resp.status_code == 201

        conflict_patch = client.patch(
            "/api/v1/prompts/custom~prompt.user.custom",
            headers=headers,
            json={"promptId": "review.review_scan.task"},
        )
        assert conflict_patch.status_code == 409
        assert conflict_patch.json()["error"]["code"] == "PROMPT_FILE_CONFLICT"


def test_custom_prompt_runtime_conflict_uses_exact_registry_id(router_api):
    prompt_id = "review.review_scan.task.custom.extension.with.long.prompt.id"

    with TestClient(router_api.app) as client:
        create_resp = client.post(
            "/api/v1/prompts/custom",
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
