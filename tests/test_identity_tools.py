from __future__ import annotations

import json

import pytest

from shinbot.agent.identity import (
    IdentityStore,
    register_identity_prompt_components,
    register_identity_tools,
)
from shinbot.agent.prompt_manager import PromptRegistry
from shinbot.agent.prompt_manager.schema import (
    PromptAssemblyRequest,
    PromptComponent,
    PromptComponentKind,
    PromptProfile,
    PromptStage,
)
from shinbot.agent.tools import ToolCallRequest, ToolManager, ToolRegistry
from shinbot.core.security.permission import PermissionEngine


@pytest.mark.asyncio
async def test_identity_set_nickname_tool_locks_prompt_name(tmp_path):
    identities_path = tmp_path / "identities.json"
    store = IdentityStore(identities_path)
    store.ensure_user(
        user_id="987654321",
        suggested_name="超长平台默认昵称",
        platform="qq",
    )

    registry = ToolRegistry()
    register_identity_tools(registry, store)
    manager = ToolManager(registry, permission_engine=PermissionEngine())

    result = await manager.execute(
        ToolCallRequest(
            tool_name="identity.set_nickname",
            arguments={
                "user_id": "987654321",
                "nickname": "咖啡",
                "aliases": ["咖啡猫"],
                "reason": "默认昵称太长",
            },
            caller="attention.workflow_runner",
            instance_id="inst",
            session_id="inst:group:g1",
        )
    )

    assert result.success is True
    assert result.output["nickname"] == "咖啡"
    assert result.output["locked"] is True

    store.ensure_user(
        user_id="987654321",
        suggested_name="又一个平台默认昵称",
        platform="qq",
    )
    payload = json.loads(identities_path.read_text(encoding="utf-8"))
    assert payload["users"][0]["name"] == "咖啡"
    assert payload["users"][0]["locked"] is True

    prompt_registry = PromptRegistry(identity_store=store)
    register_identity_prompt_components(
        prompt_registry,
        resolver=prompt_registry.resolve_builtin_identity_map_prompt,
    )
    prompt_registry.register_component(
        PromptComponent(
            id="system",
            stage=PromptStage.SYSTEM_BASE,
            kind=PromptComponentKind.STATIC_TEXT,
            content="system",
        )
    )
    prompt_registry.register_profile(PromptProfile(id="agent.default", base_components=["system"]))

    assembled = prompt_registry.assemble(
        PromptAssemblyRequest(
            profile_id="agent.default",
            context_inputs={
                "platform": "qq",
                "history_turns": [
                    {
                        "role": "user",
                        "content": "叫我短一点",
                        "sender_id": "987654321",
                        "sender_name": "又一个平台默认昵称",
                        "platform": "qq",
                    }
                ],
            },
        )
    )

    final_texts = [str(block.get("text", "")) for block in assembled.messages[-1]["content"]]
    identity_block = next(text for text in final_texts if "参与者身份参考" in text)
    assert "ID: 987654321 -> 昵称: 咖啡" in identity_block
    assert "别名: 咖啡猫" in identity_block


def test_identity_set_nickname_tool_exports_with_attention_tools(tmp_path):
    registry = ToolRegistry()
    register_identity_tools(registry, IdentityStore(tmp_path / "identities.json"))
    manager = ToolManager(registry, permission_engine=PermissionEngine())

    tools = manager.export_model_tools(
        caller="attention.workflow_runner",
        instance_id="inst",
        session_id="inst:group:g1",
        tags={"attention"},
    )

    names = {item["function"]["name"] for item in tools}
    assert "identity.set_nickname" in names
