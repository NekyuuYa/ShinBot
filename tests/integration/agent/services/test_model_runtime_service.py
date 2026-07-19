from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import threading

import pytest

from shinbot.agent.services.model_runtime import ModelCallError, ModelRuntimeCall
from shinbot.agent.services.model_runtime.backends import BackendRequestPlan
from shinbot.agent.services.model_runtime.backends.openai_compatible import OpenAICompatibleBackend
from shinbot.agent.services.model_runtime.planning import build_litellm_kwargs
from shinbot.agent.services.model_runtime.providers import (
    ModelProviderDescriptor,
    get_provider_descriptor,
    register_provider_descriptor,
)
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


class FailingNormalizerBackend(RecordingBackend):
    """Backend that succeeds remotely but fails while normalizing."""

    name = "failing-normalizer"

    def normalize_response(
        self,
        *,
        operation: str,
        response: object,
        usage: dict[str, object],
    ) -> dict[str, object]:
        raise ValueError("normalization failed")


class FirstPlanningFailureBackend(RecordingBackend):
    """Backend that fails while planning the first route member."""

    def __init__(self) -> None:
        super().__init__()
        self.planning_model_ids: list[str] = []

    def plan_request(
        self,
        *,
        provider: dict[str, object],
        model: dict[str, object],
        call: ModelRuntimeCall,
        timeout_override: float | None,
        operation: str,
    ) -> BackendRequestPlan:
        model_id = str(model["id"])
        self.planning_model_ids.append(model_id)
        if model_id == "openai-main/gpt-fast":
            raise ValueError("request planning failed")
        return super().plan_request(
            provider=provider,
            model=model,
            call=call,
            timeout_override=timeout_override,
            operation=operation,
        )


class BlockingInvokeBackend(RecordingBackend):
    """Backend whose invocation remains in its worker thread until released."""

    def __init__(self) -> None:
        super().__init__()
        self.started = threading.Event()
        self.release = threading.Event()

    def invoke(self, plan: BackendRequestPlan) -> dict[str, object]:
        self.started.set()
        self.release.wait(timeout=5.0)
        return super().invoke(plan)


class FailingThenBlockingInvokeBackend(RecordingBackend):
    """Consume most of the deadline before forcing route fallback."""

    def __init__(self) -> None:
        super().__init__()
        self.first_started = threading.Event()
        self.second_started = threading.Event()
        self.release_first = threading.Event()
        self.release_second = threading.Event()
        self._call_count = 0

    def invoke(self, plan: BackendRequestPlan) -> dict[str, object]:
        self._call_count += 1
        call_number = self._call_count
        self.plans.append(plan)
        if call_number == 1:
            self.first_started.set()
            self.release_first.wait(timeout=5.0)
            raise RuntimeError("first route member failed")
        self.second_started.set()
        self.release_second.wait(timeout=5.0)
        return {
            "model": plan.backend_model,
            "choices": [{"message": {"content": "late fallback response"}}],
            "usage": {"prompt_tokens": 2, "completion_tokens": 3},
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


def test_provider_descriptor_normalizes_runtime_messages_and_model_name():
    descriptor = get_provider_descriptor("dashscope")
    assert descriptor is not None

    normalized_messages = descriptor.normalize_runtime_messages(
        [
            {
                "role": "system",
                "content": [{"type": "text", "text": "one"}, {"type": "text", "text": "two"}],
            },
            {"role": "user", "content": "hello"},
            {"role": "system", "content": "late system"},
        ],
        backend_name="litellm",
    )

    assert normalized_messages[0]["content"] == "one\n\ntwo"
    assert [message["role"] for message in normalized_messages] == ["system", "user", "user"]

    mimo_descriptor = get_provider_descriptor("xiaomi_mimo")
    assert mimo_descriptor is not None
    assert (
        mimo_descriptor.request_model_name(
            "xiaomi_mimo/mimo-v2.5",
            backend_name="litellm",
        )
        == "mimo-v2.5"
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
async def test_normalizer_failure_does_not_double_count_success_usage(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    _seed_runtime(db)
    runtime = ModelRuntime(db, backend=FailingNormalizerBackend())

    with pytest.raises(ModelCallError, match="normalization failed"):
        await runtime.generate(
            ModelRuntimeCall(
                model_id="openai-main/gpt-fast",
                caller="agent.runtime",
                messages=[{"role": "user", "content": "Hello"}],
            )
        )

    records = db.model_executions.list_recent(limit=5)
    assert len(records) == 1
    assert records[0]["success"] is False
    assert records[0]["input_tokens"] == 0
    assert records[0]["output_tokens"] == 0

    with db.connect() as conn:
        usage = conn.execute(
            """
            SELECT total_calls, successful_calls, failed_calls, input_tokens, output_tokens
            FROM model_usage_hourly
            WHERE provider_id = ? AND model_id = ?
            """,
            ("openai-main", "openai-main/gpt-fast"),
        ).fetchone()
    assert usage is not None
    assert dict(usage) == {
        "total_calls": 1,
        "successful_calls": 0,
        "failed_calls": 1,
        "input_tokens": 0,
        "output_tokens": 0,
    }

    execution_id = records[0]["id"]
    audit_path = tmp_path / "model-audit" / f"{execution_id}.json"
    payload = json.loads(audit_path.read_text(encoding="utf-8"))
    assert payload["status"] == "error"
    assert payload["error"]["code"] == "ValueError"
    assert payload["response"]["choices"][0]["message"]["content"] == "hello from backend"


@pytest.mark.asyncio
async def test_generate_cancellation_is_audited_and_propagated(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    _seed_runtime(db)
    backend = BlockingInvokeBackend()
    runtime = ModelRuntime(db, backend=backend)
    observer_events: list[dict[str, object]] = []
    runtime.register_observer(observer_events.append)
    call_task = asyncio.create_task(
        runtime.generate(
            ModelRuntimeCall(
                model_id="openai-main/gpt-fast",
                caller="agent.runtime",
                messages=[{"role": "user", "content": "cancel me"}],
            )
        )
    )
    for _ in range(100):
        if backend.started.is_set():
            break
        await asyncio.sleep(0.01)
    assert backend.started.is_set()

    call_task.cancel()
    try:
        with pytest.raises(asyncio.CancelledError):
            await call_task
    finally:
        backend.release.set()

    records = db.model_executions.list_recent(limit=5)
    assert len(records) == 1
    assert records[0]["success"] is False
    assert records[0]["error_code"] == "CancelledError"
    assert records[0]["model_id"] == "openai-main/gpt-fast"

    audit_path = tmp_path / "model-audit" / f"{records[0]['id']}.json"
    payload = json.loads(audit_path.read_text(encoding="utf-8"))
    assert payload["status"] == "error"
    assert payload["error"]["code"] == "CancelledError"
    assert payload["request"]["model_id"] == "openai-main/gpt-fast"

    error_events = [event for event in observer_events if event.get("status") == "error"]
    assert len(error_events) == 1
    assert error_events[0]["error"]["code"] == "CancelledError"


@pytest.mark.asyncio
async def test_generate_deadline_releases_blocked_backend_and_persists_timeout(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    _seed_runtime(db)
    backend = BlockingInvokeBackend()
    runtime = ModelRuntime(db, backend=backend)
    observer_events: list[dict[str, object]] = []
    runtime.register_observer(observer_events.append)

    try:
        with pytest.raises(ModelCallError, match="Model call exceeded its 0.020s deadline"):
            await asyncio.wait_for(
                runtime.generate(
                    ModelRuntimeCall(
                        route_id="agent.default_chat",
                        caller="agent.runtime",
                        purpose="deadline_test",
                        deadline_seconds=0.02,
                        messages=[{"role": "user", "content": "deadline me"}],
                    )
                ),
                timeout=0.5,
            )
        assert backend.started.is_set()

        records = db.model_executions.list_recent(limit=5)
        assert len(records) == 1
        assert records[0]["success"] is False
        assert records[0]["error_code"] == "ModelCallDeadlineExceeded"
        assert records[0]["metadata"]["model_deadline_seconds"] == 0.02

        audit_path = tmp_path / "model-audit" / f"{records[0]['id']}.json"
        payload = json.loads(audit_path.read_text(encoding="utf-8"))
        assert payload["status"] == "error"
        assert payload["error"]["code"] == "ModelCallDeadlineExceeded"
        assert payload["request"]["deadline_seconds"] == 0.02

        error_events = [event for event in observer_events if event.get("status") == "error"]
        assert len(error_events) == 1
        assert error_events[0]["deadline_seconds"] == 0.02

        backend.release.set()
        for _ in range(100):
            if backend.plans:
                break
            await asyncio.sleep(0.01)
        assert len(backend.plans) == 1
    finally:
        backend.release.set()


@pytest.mark.asyncio
async def test_generate_deadline_limits_backend_timeout(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    _seed_runtime(db)
    backend = RecordingBackend()
    runtime = ModelRuntime(db, backend=backend)

    await runtime.generate(
        ModelRuntimeCall(
            model_id="openai-main/gpt-fast",
            caller="agent.runtime",
            deadline_seconds=3.5,
            messages=[{"role": "user", "content": "limit backend timeout"}],
        )
    )

    assert backend.plans[0].payload["timeout"] == pytest.approx(3.5, abs=0.1)
    records = db.model_executions.list_recent(limit=5)
    assert records[0]["metadata"]["model_deadline_seconds"] == 3.5


@pytest.mark.asyncio
async def test_generate_deadline_is_shared_across_route_fallbacks(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    _seed_runtime(db)
    backend = FailingThenBlockingInvokeBackend()
    runtime = ModelRuntime(db, backend=backend)
    task = asyncio.create_task(
        runtime.generate(
            ModelRuntimeCall(
                route_id="agent.default_chat",
                caller="agent.runtime",
                deadline_seconds=0.2,
                messages=[{"role": "user", "content": "shared deadline"}],
            )
        )
    )

    try:
        for _ in range(100):
            if backend.first_started.is_set():
                break
            await asyncio.sleep(0.005)
        assert backend.first_started.is_set()
        await asyncio.sleep(0.15)
        backend.release_first.set()

        for _ in range(100):
            if backend.second_started.is_set():
                break
            await asyncio.sleep(0.005)
        assert backend.second_started.is_set()
        with pytest.raises(ModelCallError, match="Model call exceeded its 0.200s deadline"):
            await asyncio.wait_for(task, timeout=0.5)

        records = db.model_executions.list_recent(limit=5)
        assert len(records) == 2
        assert records[0]["error_code"] == "ModelCallDeadlineExceeded"
        assert records[1]["error_code"] == "RuntimeError"
    finally:
        backend.release_first.set()
        backend.release_second.set()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_generate_cancellation_during_response_observer_preserves_success(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    _seed_runtime(db)
    runtime = ModelRuntime(db, backend=RecordingBackend())
    response_started = asyncio.Event()
    release_response = asyncio.Event()
    observer_events: list[dict[str, object]] = []

    async def blocking_response_observer(payload: dict[str, object]) -> None:
        observer_events.append(payload)
        if payload["event"] == "model_runtime.response":
            response_started.set()
            await release_response.wait()

    runtime.register_observer(blocking_response_observer)
    call_task = asyncio.create_task(
        runtime.generate(
            ModelRuntimeCall(
                model_id="openai-main/gpt-fast",
                caller="agent.runtime",
                messages=[{"role": "user", "content": "cancel after persistence"}],
            )
        )
    )
    await asyncio.wait_for(response_started.wait(), timeout=1.0)

    call_task.cancel()
    try:
        with pytest.raises(asyncio.CancelledError):
            await call_task
    finally:
        release_response.set()

    records = db.model_executions.list_recent(limit=5)
    assert len(records) == 1
    assert records[0]["success"] is True
    assert records[0]["error_code"] == ""

    audit_path = tmp_path / "model-audit" / f"{records[0]['id']}.json"
    payload = json.loads(audit_path.read_text(encoding="utf-8"))
    assert payload["status"] == "success"
    assert payload["error"] is None

    response_events = [
        event for event in observer_events if event.get("event") == "model_runtime.response"
    ]
    assert len(response_events) == 1
    assert response_events[0]["status"] == "success"


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
async def test_generate_records_planning_failure_and_falls_back(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    _seed_runtime(db)
    backend = FirstPlanningFailureBackend()
    runtime = ModelRuntime(db, backend=backend)
    observer_events: list[dict[str, object]] = []
    runtime.register_observer(observer_events.append)

    result = await runtime.generate(
        ModelRuntimeCall(
            route_id="agent.default_chat",
            caller="agent.runtime",
            messages=[{"role": "user", "content": "hi"}],
        )
    )

    assert result.model_id == "openai-main/gpt-backup"
    assert backend.planning_model_ids == [
        "openai-main/gpt-fast",
        "openai-main/gpt-backup",
    ]

    records = db.model_executions.list_recent(limit=5)
    assert len(records) == 2
    success_record = records[0]
    failure_record = records[1]
    assert success_record["success"] is True
    assert success_record["fallback_from_model_id"] == "openai-main/gpt-fast"
    assert failure_record["success"] is False
    assert failure_record["error_code"] == "ValueError"
    assert failure_record["error_message"] == "request planning failed"

    audit_path = tmp_path / "model-audit" / f"{failure_record['id']}.json"
    payload = json.loads(audit_path.read_text(encoding="utf-8"))
    assert payload["status"] == "error"
    assert payload["error"] == {
        "code": "ValueError",
        "message": "request planning failed",
    }
    assert payload["request"]["model_id"] == "openai-main/gpt-fast"
    assert payload["request"]["kwargs"] == {}

    error_events = [event for event in observer_events if event.get("status") == "error"]
    assert len(error_events) == 1
    assert error_events[0]["model_id"] == "openai-main/gpt-fast"
    assert error_events[0]["error"] == {
        "code": "ValueError",
        "message": "request planning failed",
    }


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


def test_openai_compatible_backend_plans_provider_and_embedding_input():
    backend = OpenAICompatibleBackend()
    provider = {
        "id": "custom-openai-main",
        "type": "custom_openai",
        "base_url": "https://api.example.com/v1",
        "auth": {"api_key": "secret-key"},
        "default_params": {
            "temperature": 0.2,
            "requestHeaders": {"HTTP-Referer": "https://shinbot.example"},
        },
    }
    model = {
        "backend_model": "text-embedding-3-small",
        "default_params": {"encoding_format": "float"},
    }

    plan = backend.plan_request(
        provider=provider,
        model=model,
        call=ModelRuntimeCall(
            model_id="custom-openai-main/text-embedding",
            caller="test.embedding",
            input_data="hello world",
            params={"dimensions": 256},
        ),
        timeout_override=10.0,
        operation="embedding",
    )

    assert plan.payload["input"] == "hello world"
    assert plan.payload["model"] == "text-embedding-3-small"
    assert plan.payload["encoding_format"] == "float"
    assert plan.payload["dimensions"] == 256
    assert plan.payload["timeout"] == 10.0
    assert plan.payload["extra_headers"] == {"HTTP-Referer": "https://shinbot.example"}
    assert "requestHeaders" not in plan.payload
    assert plan.metadata["provider"]["base_url"] == "https://api.example.com/v1"
    assert plan.metadata["provider"]["auth"] == {"api_key": "secret-key"}


def test_openai_compatible_backend_strips_prefix_filters_completion_params_and_redacts_headers():
    backend = OpenAICompatibleBackend()
    plan = backend.plan_request(
        provider={
            "id": "openai-main",
            "type": "openai",
            "base_url": "https://api.openai.com/v1",
            "auth": {"api_key": "secret-key"},
            "default_params": {
                "temperature": 0.2,
                "requestHeaders": {
                    "Authorization": "Bearer provider-secret",
                    "X-Provider": "provider",
                },
                "allowed_openai_params": ["metadata"],
            },
        },
        model={
            "backend_model": "openai/gpt-4.1-mini",
            "default_params": {
                "max_tokens": 64,
                "extra_headers": {"X-Model": "model"},
            },
        },
        call=ModelRuntimeCall(
            model_id="openai-main/gpt-fast",
            caller="test.completion",
            messages=[{"role": "user", "content": "hello"}],
            params={
                "drop_params": True,
                "extra_headers": {"X-Call": "call"},
                "num_retries": 2,
                "presence_penalty": 0.5,
            },
        ),
        timeout_override=5.0,
        operation="completion",
    )

    assert plan.payload["model"] == "gpt-4.1-mini"
    assert plan.payload["temperature"] == 0.2
    assert plan.payload["max_tokens"] == 64
    assert plan.payload["presence_penalty"] == 0.5
    assert plan.payload["timeout"] == 5.0
    assert plan.payload["extra_headers"] == {
        "Authorization": "Bearer provider-secret",
        "X-Call": "call",
        "X-Model": "model",
        "X-Provider": "provider",
    }
    assert "allowed_openai_params" not in plan.payload
    assert "drop_params" not in plan.payload
    assert "num_retries" not in plan.payload
    assert plan.safe_payload["extra_headers"] == {
        "Authorization": "***",
        "X-Call": "call",
        "X-Model": "model",
        "X-Provider": "provider",
    }


def test_openai_compatible_backend_filters_embedding_params_and_strips_provider_prefix():
    backend = OpenAICompatibleBackend()
    plan = backend.plan_request(
        provider={
            "id": "openrouter-main",
            "type": "openrouter",
            "base_url": "https://openrouter.ai/api/v1",
            "auth": {"api_key": "secret-key"},
            "default_params": {"temperature": 0.2},
        },
        model={
            "backend_model": "openrouter/google/gemma-4-31b-it:free",
            "default_params": {"max_tokens": 32},
        },
        call=ModelRuntimeCall(
            model_id="openrouter-main/gemma",
            caller="test.embedding",
            input_data="hello world",
            params={"dimensions": 256, "user": "tester"},
        ),
        timeout_override=3.0,
        operation="embedding",
    )

    assert plan.payload == {
        "model": "google/gemma-4-31b-it:free",
        "input": "hello world",
        "dimensions": 256,
        "user": "tester",
        "timeout": 3.0,
    }


def test_openai_compatible_backend_uses_descriptor_auth_param_key(monkeypatch: pytest.MonkeyPatch):
    descriptor = get_provider_descriptor("custom_openai")
    assert descriptor is not None
    backend = OpenAICompatibleBackend()
    captured: dict[str, str] = {}

    class FakeOpenAI:
        def __init__(self, *, api_key: str, base_url: str) -> None:
            captured["api_key"] = api_key
            captured["base_url"] = base_url

    monkeypatch.setattr(
        "shinbot.agent.services.model_runtime.backends.openai_compatible.backend.OpenAI",
        FakeOpenAI,
    )

    backend._get_client(
        {
            "type": "custom_openai",
            "base_url": "https://api.example.com/v1",
            "auth": {"api_key": "secret-key"},
        }
    )

    assert captured == {
        "api_key": "secret-key",
        "base_url": "https://api.example.com/v1",
    }


def test_openai_compatible_backend_uses_descriptor_request_header_keys():
    register_provider_descriptor(
        ModelProviderDescriptor(
            provider_type="test_header_provider",
            supported_backends=frozenset({"openai_compatible"}),
            auth_strategy="none",
            request_headers_param_keys=("provider_headers",),
            catalog_path=None,
        )
    )

    backend = OpenAICompatibleBackend()
    plan = backend.plan_request(
        provider={
            "id": "test-header-provider",
            "type": "test_header_provider",
            "base_url": "https://api.example.com/v1",
            "auth": {},
            "default_params": {"provider_headers": {"X-Provider": "provider"}},
        },
        model={
            "backend_model": "demo-model",
            "default_params": {},
        },
        call=ModelRuntimeCall(
            model_id="demo/model",
            caller="agent.runtime",
            messages=[{"role": "user", "content": "Hello"}],
            params={"provider_headers": {"X-Call": "call"}},
        ),
        timeout_override=3.0,
        operation="completion",
    )

    assert plan.payload["extra_headers"] == {"X-Provider": "provider", "X-Call": "call"}
    assert "provider_headers" not in plan.payload


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
