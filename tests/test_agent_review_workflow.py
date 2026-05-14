from __future__ import annotations

import asyncio
from typing import Any

import pytest

from shinbot.agent.coordinators.active_chat import ActiveChatCoordinator
from shinbot.agent.coordinators.dispatcher import ActiveReplyDispatcher
from shinbot.agent.coordinators.review import ReviewCoordinator
from shinbot.agent.coordinators.review.factory import (
    ReviewRunnerFactory,
    ReviewRuntimeConfig,
    ReviewStageRuntimeConfig,
    register_review_prompt_components,
)
from shinbot.agent.coordinators.review.models import (
    ReviewWorkflowConfig,
    build_review_workflow_explanation,
)
from shinbot.agent.runners._review_base import ReviewLLMRunnerConfig
from shinbot.agent.runners.review_block_digest import LLMReviewBlockDigestStageRunner
from shinbot.agent.runners.review_bootstrap import LLMActiveChatBootstrapStageRunner
from shinbot.agent.runners.review_compression import LLMOverflowCompressionStageRunner
from shinbot.agent.runners.review_models import (
    ActiveChatBootstrapStageOutput,
    OverflowCompressionStageOutput,
    ReplyDecisionStageOutput,
    ReviewBlockDigestStageOutput,
    ReviewScanStageOutput,
)
from shinbot.agent.runners.review_reply import LLMReplyDecisionStageRunner
from shinbot.agent.runners.review_scan import LLMReviewScanStageRunner
from shinbot.agent.runtime.review_stores import (
    DatabaseReviewMessageStore,
    DatabaseReviewSummaryStore,
)
from shinbot.agent.runtime.tool_config import StageToolConfig
from shinbot.agent.scheduler import (
    ActiveChatBootstrapApplyDecision,
    ActiveChatDisposition,
    ActiveChatState,
    AgentScheduler,
    AgentState,
)
from shinbot.agent.scheduler.models import (
    ReviewCompletionDecision,
    ReviewPlan,
    UnreadMessage,
    UnreadRange,
)
from shinbot.agent.services.context.review_context_builder import (
    ReviewContextBuilderAdapter,
    ReviewStageInput,
)
from shinbot.agent.services.prompt_engine import (
    PromptComponent,
    PromptComponentKind,
    PromptRegistry,
    PromptStage,
)
from shinbot.agent.utils.parsing import parse_json_object
from shinbot.agent.workflows.active_chat.prompt_registration import (
    register_active_chat_prompt_components,
)
from shinbot.core.dispatch.dispatchers import AgentEntrySignal
from shinbot.persistence import DatabaseManager
from shinbot.persistence.records import MessageLogRecord


def _make_prompt_registry() -> PromptRegistry:
    registry = PromptRegistry()
    register_review_prompt_components(registry)
    register_active_chat_prompt_components(registry)
    return registry


class FixedReviewPolicy:
    def initial_plan(self, *, session_id: str, now: float) -> ReviewPlan:
        return ReviewPlan(
            session_id=session_id,
            next_review_at=now,
            reason="fixed_due_review",
            updated_at=now,
        )

    def plan_after_review(
        self,
        *,
        session_id: str,
        now: float,
        previous_plan: ReviewPlan | None = None,
    ) -> ReviewPlan:
        return ReviewPlan(
            session_id=session_id,
            next_review_at=now + 60.0,
            reason="fixed_after_review",
            updated_at=now,
        )


class FakeReviewScheduler:
    def __init__(self) -> None:
        self.complete_review_calls: list[dict[str, object]] = []
        self.apply_bootstrap_calls: list[dict[str, object]] = []

    def unread_ranges(self, session_id: str, *, limit: int = 50) -> list[UnreadRange]:
        return [
            UnreadRange(
                id=1,
                session_id=session_id,
                start_msg_log_id=1,
                end_msg_log_id=2,
                start_at=10.0,
                end_at=11.0,
                message_count=2,
            ),
            UnreadRange(
                id=2,
                session_id=session_id,
                start_msg_log_id=3,
                end_msg_log_id=5,
                start_at=12.0,
                end_at=14.0,
                message_count=3,
            ),
        ][:limit]

    def count_unread_messages(self, session_id: str) -> int:
        return 5

    def complete_review(
        self,
        session_id: str,
        *,
        enter_active_chat: bool = False,
        active_chat_initial_interest: float | None = None,
        active_chat_decay_half_life_seconds: float | None = None,
        next_review_plan: ReviewPlan | None = None,
        now: float | None = None,
    ) -> ReviewCompletionDecision:
        self.complete_review_calls.append(
            {
                "session_id": session_id,
                "enter_active_chat": enter_active_chat,
                "active_chat_initial_interest": active_chat_initial_interest,
                "active_chat_decay_half_life_seconds": active_chat_decay_half_life_seconds,
                "next_review_plan": next_review_plan,
                "now": now,
            }
        )
        return ReviewCompletionDecision(
            session_id=session_id,
            state=AgentState.ACTIVE_CHAT,
            active_chat_state=ActiveChatState(
                session_id=session_id,
                interest_value=active_chat_initial_interest or 0.0,
                decay_half_life_seconds=active_chat_decay_half_life_seconds or 0.0,
                entered_at=0.0,
                updated_at=0.0,
            ),
            active_chat_started=True,
        )

    def apply_active_chat_bootstrap(
        self,
        session_id: str,
        *,
        disposition: ActiveChatDisposition,
        active_epoch: int | None = None,
        now: float | None = None,
    ) -> ActiveChatBootstrapApplyDecision:
        self.apply_bootstrap_calls.append(
            {
                "session_id": session_id,
                "disposition": disposition,
                "active_epoch": active_epoch,
                "now": now,
            }
        )
        return ActiveChatBootstrapApplyDecision(
            session_id=session_id,
            state=AgentState.ACTIVE_CHAT,
            active_chat_state=ActiveChatState(
                session_id=session_id,
                interest_value=20.0,
                decay_half_life_seconds=10.0,
                entered_at=0.0,
                updated_at=0.0,
                bootstrap_applied=True,
                bootstrap_disposition=disposition,
            ),
            bootstrap_applied=True,
        )


class RecordingReviewContextBuilder:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def build_for_messages(
        self,
        *,
        session_id: str,
        messages: list[dict],
        purpose: str,
        options=None,
    ):
        self.calls.append(
            {
                "session_id": session_id,
                "message_ids": [message["id"] for message in messages],
                "purpose": purpose,
                "metadata": dict(options.metadata) if options is not None else {},
                "previous_summary": options.previous_summary if options is not None else "",
            }
        )
        return None


class SelectingReviewScanRunner:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def run(self, stage_input) -> ReviewScanStageOutput:
        message_ids = [message["id"] for message in stage_input.source_messages]
        self.calls.append(
            {
                "purpose": stage_input.purpose,
                "message_ids": message_ids,
                "metadata": dict(stage_input.metadata),
            }
        )
        return ReviewScanStageOutput(
            candidate_message_ids=[message_ids[-1], message_ids[-1]] if message_ids else [],
            reason=f"selected_from_{len(message_ids)}",
        )


class YieldingReviewScanRunner(SelectingReviewScanRunner):
    async def run(self, stage_input) -> ReviewScanStageOutput:
        await asyncio.sleep(0)
        return await super().run(stage_input)


class RecordingOverflowCompressionRunner:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def run(self, stage_input) -> OverflowCompressionStageOutput:
        message_ids = [message["id"] for message in stage_input.source_messages]
        self.calls.append(
            {
                "purpose": stage_input.purpose,
                "message_ids": message_ids,
                "metadata": dict(stage_input.metadata),
            }
        )
        return OverflowCompressionStageOutput(
            summary="older messages summarized",
            candidate_message_ids=[message_ids[0]] if message_ids else [],
            reason="compressed_old_messages",
        )


class RecordingReplyDecisionRunner:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def run(self, stage_input) -> ReplyDecisionStageOutput:
        message_ids = [message["id"] for message in stage_input.source_messages]
        candidate_id = stage_input.metadata["candidate_message_id"]
        candidate_ids = stage_input.metadata.get("candidate_message_ids", [candidate_id])
        self.calls.append(
            {
                "purpose": stage_input.purpose,
                "candidate_id": candidate_id,
                "message_ids": message_ids,
                "metadata": dict(stage_input.metadata),
            }
        )
        if not isinstance(candidate_ids, list):
            candidate_ids = [candidate_id]
        reason_target = (
            str(candidate_ids[0])
            if len(candidate_ids) == 1
            else ",".join(str(item) for item in candidate_ids)
        )
        return ReplyDecisionStageOutput(
            replied=False,
            target_message_ids=[item for item in candidate_ids if isinstance(item, int)],
            reason=f"checked_{reason_target}",
        )


class RecordingBlockDigestRunner:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def run(self, stage_input) -> ReviewBlockDigestStageOutput:
        message_ids = [message["id"] for message in stage_input.source_messages]
        self.calls.append(
            {
                "purpose": stage_input.purpose,
                "message_ids": message_ids,
                "metadata": dict(stage_input.metadata),
            }
        )
        return ReviewBlockDigestStageOutput(
            summary=f"digest_{stage_input.metadata['block_index']}",
            reason="recorded_digest",
        )


class SlowBlockDigestRunner:
    def __init__(self) -> None:
        self.active_count = 0
        self.max_active_count = 0

    async def run(self, stage_input) -> ReviewBlockDigestStageOutput:
        self.active_count += 1
        self.max_active_count = max(self.max_active_count, self.active_count)
        await asyncio.sleep(0)
        self.active_count -= 1
        return ReviewBlockDigestStageOutput(
            summary=f"digest_{stage_input.metadata['block_index']}",
            reason="slow_digest",
        )


class FailingBlockDigestRunner:
    async def run(self, stage_input) -> ReviewBlockDigestStageOutput:
        raise RuntimeError("digest failed")


class FixedCandidateScanRunner:
    def __init__(self, candidate_message_ids: list[int]) -> None:
        self.candidate_message_ids = candidate_message_ids

    async def run(self, stage_input) -> ReviewScanStageOutput:
        return ReviewScanStageOutput(
            candidate_message_ids=list(self.candidate_message_ids),
            reason="fixed_candidates",
        )


class RecordingActiveChatBootstrapRunner:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def run(self, stage_input) -> ActiveChatBootstrapStageOutput:
        message_ids = [message["id"] for message in stage_input.source_messages]
        self.calls.append(
            {
                "purpose": stage_input.purpose,
                "message_ids": message_ids,
                "metadata": dict(stage_input.metadata),
            }
        )
        return ActiveChatBootstrapStageOutput(
            disposition=ActiveChatDisposition.ENGAGED,
            reason="bootstrap_selected_interest",
        )


class FakeContextManager:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def build_instruction_stage_content(
        self,
        session_id: str,
        unread_records: list[dict],
        *,
        previous_summary: str = "",
        self_platform_id: str = "",
        now_ms: int | None = None,
    ) -> list[dict]:
        self.calls.append(
            {
                "session_id": session_id,
                "message_ids": [record["id"] for record in unread_records],
                "previous_summary": previous_summary,
                "self_platform_id": self_platform_id,
                "now_ms": now_ms,
            }
        )
        return [{"type": "text", "text": f"{len(unread_records)} messages"}]


class FakeModelRuntime:
    def __init__(self, responses: list[str | dict]) -> None:
        self.responses = list(responses)
        self.calls: list[object] = []

    async def generate(self, call):
        self.calls.append(call)
        response = self.responses.pop(0)
        if isinstance(response, dict):
            text = str(response.get("text", "") or "")
            tool_calls = list(response.get("tool_calls", []))
        else:
            text = response
            tool_calls = []
        return type(
            "FakeGenerateResult",
            (),
            {
                "text": text,
                "tool_calls": tool_calls,
                "raw_response": None,
                "execution_id": "exec-1",
                "route_id": "",
                "provider_id": "",
                "model_id": "",
                "usage": {},
            },
        )()


class FakeReviewToolManager:
    def __init__(self) -> None:
        self.execute_calls: list[object] = []
        self.build_request_tool_calls: list[dict[str, object]] = []
        self.export_model_tool_calls: list[dict[str, object]] = []
        self._next_message_log_id = 42

    def export_model_tools(self, **kwargs):
        self.export_model_tool_calls.append(dict(kwargs))
        tools = [
            {
                "type": "function",
                "function": {
                    "name": "no_reply",
                    "description": "do not reply",
                    "parameters": {"type": "object", "properties": {}, "required": []},
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "send_reply",
                    "description": "send reply",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "text": {"type": "string"},
                            "quote_message_log_id": {"type": "integer"},
                        },
                        "required": ["text"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "attention.inspect_state",
                    "description": "other attention tool",
                    "parameters": {"type": "object", "properties": {}, "required": []},
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "send_poke",
                    "description": "send poke",
                    "parameters": {
                        "type": "object",
                        "properties": {"user_id": {"type": "string"}},
                        "required": ["user_id"],
                    },
                },
            },
        ]
        if kwargs.get("tags") == {"knowledge"}:
            return [
                tool
                for tool in tools
                if tool["function"]["name"] in {"attention.inspect_state", "send_reply"}
            ]
        return tools

    def build_request_tools(self, tool_names, **kwargs):
        self.build_request_tool_calls.append({"tool_names": list(tool_names), **kwargs})
        schemas = {
            str(item["function"]["name"]): item
            for item in self.export_model_tools(**kwargs)
        }
        return [schemas[name] for name in tool_names if name in schemas]

    async def execute(self, call):
        self.execute_calls.append(call)
        output = {"sent": True}
        if call.tool_name == "send_reply":
            output["message_log_id"] = self._next_message_log_id
            self._next_message_log_id += 1
        return type(
            "FakeToolCallResult",
            (),
            {
                "success": True,
                "output": output,
                "error_code": "",
                "error_message": "",
            },
        )()


class FakeSummaryService:
    def __init__(self) -> None:
        self.overflow_compressions: list[dict[str, object]] = []
        self.block_digests: list[dict[str, object]] = []
        self.session_summaries: list[object] = []

    def save_overflow_compression(self, *args, **kwargs) -> int:
        self.overflow_compressions.append({"args": args, "kwargs": kwargs})
        return len(self.overflow_compressions)

    def save_block_digest(self, *args, **kwargs) -> int:
        self.block_digests.append({"args": args, "kwargs": kwargs})
        return len(self.block_digests)

    def list_by_session(self, session_id, **kwargs):
        return list(self.session_summaries)[: kwargs.get("limit", 50)]

    def get_latest_by_session(self, session_id, **kwargs):
        if not self.session_summaries:
            return None
        return self.session_summaries[-1]


def _insert_message(
    db: DatabaseManager,
    *,
    session_id: str = "bot:group:room",
    raw_text: str,
    created_at: float,
) -> int:
    return db.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            platform_msg_id=f"msg-{raw_text}",
            sender_id="user-1",
            sender_name="User",
            raw_text=raw_text,
            content_json="[]",
            role="user",
            created_at=created_at,
        )
    )


def _strip_run_id_from_calls(calls: list[dict]) -> list[dict]:
    """Remove dynamic review_run_id from recorded call metadata for comparison."""
    result = []
    for call in calls:
        call_copy = dict(call)
        if "metadata" in call_copy and isinstance(call_copy["metadata"], dict):
            call_copy["metadata"] = {
                k: v for k, v in call_copy["metadata"].items() if k != "review_run_id"
            }
        result.append(call_copy)
    return result


def test_database_review_store_reads_review_windows(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    message_ids = [
        _insert_message(db, raw_text=f"m{index}", created_at=float(index * 1000))
        for index in range(1, 6)
    ]
    store = DatabaseReviewMessageStore(db)
    unread_range = UnreadRange(
        id=1,
        session_id="bot:group:room",
        start_msg_log_id=message_ids[1],
        end_msg_log_id=message_ids[3],
        start_at=2000.0,
        end_at=4000.0,
        message_count=3,
    )

    range_rows = store.list_for_unread_range(unread_range, limit=2, offset=1)
    around_rows = store.list_around_message(
        session_id="bot:group:room",
        message_log_id=message_ids[2],
        before=1,
        after=2,
    )
    time_rows = store.list_by_time(
        session_id="bot:group:room",
        start_at=2000.0,
        end_at=5000.0,
        limit=10,
    )

    assert [row["raw_text"] for row in range_rows] == ["m3", "m4"]
    assert [row["raw_text"] for row in around_rows] == ["m2", "m3", "m4", "m5"]
    assert [row["raw_text"] for row in time_rows] == ["m2", "m3", "m4", "m5"]


def test_review_context_builder_adapter_keeps_source_messages_structured() -> None:
    context_manager = FakeContextManager()
    adapter = ReviewContextBuilderAdapter(context_manager)

    stage_input = adapter.build_for_messages(
        session_id="bot:group:room",
        messages=[{"id": 1, "raw_text": "hello"}],
        purpose="review_scan",
        options=None,
    )

    assert stage_input.session_id == "bot:group:room"
    assert stage_input.purpose == "review_scan"
    assert stage_input.source_messages == [{"id": 1, "raw_text": "hello"}]
    assert stage_input.instruction_content == []
    assert stage_input.metadata == {"purpose": "review_scan"}
    assert context_manager.calls == []


def test_review_llm_json_parser_accepts_fenced_object() -> None:
    payload = parse_json_object('```json\n{"candidate_message_ids": [1], "reason": "ok"}\n```')

    assert payload == {"candidate_message_ids": [1], "reason": "ok"}


@pytest.mark.asyncio
async def test_review_llm_stage_runners_parse_structured_outputs() -> None:
    model_runtime = FakeModelRuntime(
        [
            '{"summary": "old context", "candidate_message_ids": [1, "2"], "reason": "compressed"}',
            '{"candidate_message_ids": [3, 3], "reason": "selected"}',
            '{"replied": true, "reply_message_id": 10, "target_message_ids": [3], "reason": "reply"}',
            '{"disposition": "engaged", "reason": "chat"}',
        ]
    )
    config = ReviewLLMRunnerConfig(
        route_id="route-a",
        model_id="model-a",
        caller="test.review",
    )
    stage_input = ReviewStageInput(
        session_id="bot:group:room",
        purpose="review_scan",
        source_messages=[{"id": 1, "raw_text": "hello"}],
        metadata={"candidate_message_id": 3},
    )
    prompt_registry = _make_prompt_registry()

    compression = await LLMOverflowCompressionStageRunner(
        model_runtime,
        config=config,
        prompt_registry=prompt_registry,
    ).run(stage_input)
    scan = await LLMReviewScanStageRunner(
        model_runtime,
        config=config,
        prompt_registry=prompt_registry,
    ).run(stage_input)
    reply = await LLMReplyDecisionStageRunner(
        model_runtime,
        config=config,
        prompt_registry=prompt_registry,
    ).run(stage_input)
    bootstrap = await LLMActiveChatBootstrapStageRunner(
        model_runtime,
        config=config,
        prompt_registry=prompt_registry,
    ).run(stage_input)

    assert compression.summary == "old context"
    assert compression.candidate_message_ids == [1, 2]
    assert compression.reason == "compressed"
    assert scan.candidate_message_ids == [3, 3]
    assert scan.reason == "selected"
    assert reply.replied is True
    assert reply.reply_message_id == 10
    assert reply.reply_message_ids == [10]
    assert reply.target_message_ids == [3]
    assert bootstrap.disposition == ActiveChatDisposition.ENGAGED
    assert model_runtime.calls[0].route_id == "route-a"
    assert model_runtime.calls[0].model_id == "model-a"
    assert model_runtime.calls[0].caller == "test.review"
    assert model_runtime.calls[0].instance_id == "bot"
    assert model_runtime.calls[0].response_format["type"] == "json_schema"
    assert model_runtime.calls[0].metadata["candidate_message_id"] == 3


@pytest.mark.asyncio
async def test_review_compression_runner_saves_summary() -> None:
    summary_service = FakeSummaryService()
    model_runtime = FakeModelRuntime(
        ['{"summary": "older context", "candidate_message_ids": [2], "reason": "compressed"}']
    )
    runner = LLMOverflowCompressionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        summary_service=summary_service,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="overflow_compression",
            source_messages=[{"id": 1, "raw_text": "old"}, {"id": 2, "raw_text": "older"}],
            metadata={
                "review_run_id": "review-1",
                "start_msg_log_id": 1,
                "end_msg_log_id": 2,
                "message_count": 2,
            },
        )
    )

    assert result.summary == "older context"
    assert result.candidate_message_ids == [2]
    saved = summary_service.overflow_compressions[0]
    assert saved["args"] == ()
    assert saved["kwargs"]["session_id"] == "bot:group:room"
    assert saved["kwargs"]["source_run_id"] == "review-1"
    assert saved["kwargs"]["content"] == "older context"
    assert saved["kwargs"]["msg_log_start"] == 1
    assert saved["kwargs"]["msg_log_end"] == 2
    assert saved["kwargs"]["msg_count"] == 2
    assert saved["kwargs"]["metadata"]["candidate_message_ids"] == [2]


@pytest.mark.asyncio
async def test_review_block_digest_runner_saves_block_digest() -> None:
    summary_service = FakeSummaryService()
    model_runtime = FakeModelRuntime(
        ['{"summary": "block context", "reason": "digest"}']
    )
    runner = LLMReviewBlockDigestStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        summary_service=summary_service,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="review_block_digest",
            source_messages=[{"id": 10, "raw_text": "hello"}],
            metadata={
                "review_run_id": "review-1",
                "block_index": 3,
                "start_msg_log_id": 10,
                "end_msg_log_id": 19,
                "message_count": 10,
            },
        )
    )

    assert isinstance(result, ReviewBlockDigestStageOutput)
    assert result.summary == "block context"
    saved = summary_service.block_digests[0]
    assert saved["args"] == ()
    assert saved["kwargs"]["session_id"] == "bot:group:room"
    assert saved["kwargs"]["source_run_id"] == "review-1"
    assert saved["kwargs"]["block_index"] == 3
    assert saved["kwargs"]["content"] == "block context"
    assert saved["kwargs"]["msg_log_start"] == 10
    assert saved["kwargs"]["msg_log_end"] == 19
    assert saved["kwargs"]["msg_count"] == 10


@pytest.mark.asyncio
async def test_review_llm_runner_uses_prompt_registry_when_available() -> None:
    model_runtime = FakeModelRuntime(['{"candidate_message_ids": [7], "reason": "selected"}'])
    runner = LLMReviewScanStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(),
        prompt_registry=_make_prompt_registry(),
    )
    stage_input = ReviewStageInput(
        session_id="bot:group:room",
        purpose="review_scan",
        source_messages=[{"id": 7, "raw_text": "hello"}],
        instruction_content=[{"type": "text", "text": "rendered context"}],
        metadata={"batch": 1},
    )

    result = await runner.run(stage_input)

    assert result.candidate_message_ids == [7]
    call = model_runtime.calls[0]
    system_text = " ".join(block["text"] for block in call.messages[0]["content"])
    user_text = " ".join(block["text"] for block in call.messages[-1]["content"])
    assert "ShinBot Agent 审查流程的内部阶段" in system_text
    assert "评估提供的未读消息" in user_text
    assert "rendered context" in user_text
    assert call.tools == []
    assert call.metadata["workflow_id"] == "review"
    assert call.metadata["stage_id"] == "review_scan"
    assert call.metadata["review_stage"] == "review_scan"
    assert call.metadata["batch"] == 1


@pytest.mark.asyncio
async def test_reply_decision_runner_exports_and_executes_terminal_tools() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "send_reply",
                            "arguments": '{"text": "hello", "quote_message_log_id": 7}',
                        },
                    }
                ]
            }
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}],
            metadata={"candidate_message_ids": [7]},
        )
    )

    call = model_runtime.calls[0]
    tool_names = [tool["function"]["name"] for tool in call.tools]
    assert tool_names == ["no_reply", "send_reply", "send_poke"]
    assert tool_manager.build_request_tool_calls[0]["tags"] == {"chat_action"}
    send_reply_tool = call.tools[1]
    send_poke_tool = call.tools[2]
    assert "quote_message_log_id" not in send_reply_tool["function"]["parameters"]["required"]
    assert "first send_reply" in send_reply_tool["function"]["description"]
    assert "only takes effect after at least one send_reply" in send_poke_tool["function"][
        "description"
    ]
    assert call.response_format is None
    assert result.replied is True
    assert result.reply_message_id == 42
    assert result.reply_message_ids == [42]
    assert result.target_message_ids == [7]
    assert result.reason == "send_reply_tool"
    assert tool_manager.execute_calls[0].tool_name == "send_reply"
    assert tool_manager.execute_calls[0].caller == "test.review"


@pytest.mark.asyncio
async def test_reply_decision_runner_adds_configured_extra_tools() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {"name": "no_reply", "arguments": "{}"},
                    }
                ]
            }
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(
            caller="test.review",
            tool_config=StageToolConfig(
                extra_names=("attention.inspect_state",),
                extra_tags=("knowledge",),
            ),
        ),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}],
            metadata={"candidate_message_ids": [7]},
        )
    )

    call = model_runtime.calls[0]
    tool_names = [tool["function"]["name"] for tool in call.tools]
    assert tool_names == [
        "no_reply",
        "send_reply",
        "send_poke",
        "attention.inspect_state",
    ]
    assert tool_manager.build_request_tool_calls[0]["tags"] == {"chat_action"}
    assert "tags" not in tool_manager.build_request_tool_calls[1]
    assert tool_manager.export_model_tool_calls[-1]["tags"] == {"knowledge"}
    assert tool_manager.execute_calls == []


@pytest.mark.asyncio
async def test_reply_decision_runner_executes_multiple_replies_in_order() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "send_reply",
                            "arguments": '{"text": "first", "quote_message_log_id": 7}',
                        },
                    },
                    {
                        "id": "tool-2",
                        "function": {
                            "name": "send_reply",
                            "arguments": '{"text": "second"}',
                        },
                    },
                ]
            }
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}, {"id": 8, "raw_text": "world"}],
            metadata={"candidate_message_ids": [7, 8]},
        )
    )

    assert result.replied is True
    assert result.reply_message_id == 42
    assert result.reply_message_ids == [42, 43]
    assert result.target_message_ids == [7, 8]
    assert result.reason == "send_reply_tool:2"
    assert [call.tool_name for call in tool_manager.execute_calls] == [
        "send_reply",
        "send_reply",
    ]
    assert [call.arguments["text"] for call in tool_manager.execute_calls] == [
        "first",
        "second",
    ]
    assert tool_manager.execute_calls[0].arguments["quote_message_log_id"] == 7
    assert "quote_message_log_id" not in tool_manager.execute_calls[1].arguments
    assert [call.arguments["idempotency_key"] for call in tool_manager.execute_calls] == [
        "exec-1:0",
        "exec-1:1",
    ]


@pytest.mark.asyncio
async def test_reply_decision_runner_allows_poke_after_reply_only() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "send_poke",
                            "arguments": '{"user_id": "user-1"}',
                        },
                    },
                    {
                        "id": "tool-2",
                        "function": {
                            "name": "send_reply",
                            "arguments": '{"text": "hello", "quote_message_log_id": 7}',
                        },
                    },
                    {
                        "id": "tool-3",
                        "function": {
                            "name": "send_poke",
                            "arguments": '{"user_id": "user-1"}',
                        },
                    },
                ]
            }
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}],
            metadata={"candidate_message_ids": [7]},
        )
    )

    assert result.replied is True
    assert result.reply_message_ids == [42]
    assert result.reason == "send_reply_tool:1;send_poke_tool:2"
    assert [call.tool_name for call in tool_manager.execute_calls] == [
        "send_poke",
        "send_reply",
        "send_poke",
    ]


@pytest.mark.asyncio
async def test_reply_decision_runner_ignores_standalone_poke() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "send_poke",
                            "arguments": '{"user_id": "user-1"}',
                        },
                    }
                ]
            }
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}],
            metadata={"candidate_message_ids": [7]},
        )
    )

    assert result.replied is False
    assert result.reason == "llm_reply_decision_no_terminal_tool"
    assert tool_manager.execute_calls == []


@pytest.mark.asyncio
async def test_reply_decision_runner_requires_quoted_reply_message() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "send_reply",
                            "arguments": '{"text": "hello"}',
                        },
                    }
                ]
            }
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}],
            metadata={"candidate_message_ids": [7]},
        )
    )

    assert result.replied is False
    assert result.target_message_ids == [7]
    assert result.reason == "reply_tool_missing_quote_message_log_id"
    assert tool_manager.execute_calls == []


@pytest.mark.asyncio
async def test_reply_decision_runner_requires_first_quote_to_target_candidate() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "send_reply",
                            "arguments": '{"text": "hello", "quote_message_log_id": 99}',
                        },
                    }
                ]
            }
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}, {"id": 99, "raw_text": "nearby"}],
            metadata={"candidate_message_ids": [7]},
        )
    )

    assert result.replied is False
    assert result.target_message_ids == [7]
    assert result.reason == "reply_tool_quote_message_log_id_not_candidate"
    assert tool_manager.execute_calls == []


@pytest.mark.asyncio
async def test_reply_decision_runner_repairs_toolless_text_response() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(
        [
            "我应该回复一下",
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "send_reply",
                            "arguments": '{"text": "hello", "quote_message_log_id": 7}',
                        },
                    }
                ]
            },
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}],
            metadata={"candidate_message_ids": [7]},
        )
    )

    assert result.replied is True
    assert result.reply_message_ids == [42]
    assert result.reason == "send_reply_tool"
    assert len(model_runtime.calls) == 2
    repair_call = model_runtime.calls[1]
    assert repair_call.metadata["repair_attempt"] == 1
    assert repair_call.metadata["repair_reason"] == "reply_decision_toolless_output"
    assert repair_call.messages[-2] == {
        "role": "assistant",
        "content": "我应该回复一下",
    }
    repair_text = repair_call.messages[-1]["content"][0]["text"]
    assert "必须调用工具" in repair_text
    assert "第一条 send_reply 必须带 quote_message_log_id" in repair_text
    assert tool_manager.execute_calls[0].tool_name == "send_reply"


@pytest.mark.asyncio
async def test_reply_decision_runner_uses_configured_repair_prompt() -> None:
    prompt_registry = _make_prompt_registry()
    prompt_registry.register_component(
        PromptComponent(
            id="custom.reply.repair",
            stage=PromptStage.INSTRUCTIONS,
            kind=PromptComponentKind.STATIC_TEXT,
            content="Custom reply repair prompt.",
        )
    )
    model_runtime = FakeModelRuntime(
        [
            "raw text",
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "no_reply",
                            "arguments": '{"internal_summary": "fixed"}',
                        },
                    }
                ]
            },
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(
            caller="test.review",
            special_prompt_ids={"repair": "custom.reply.repair"},
        ),
        prompt_registry=prompt_registry,
        tool_manager=FakeReviewToolManager(),
    )

    await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}],
            metadata={"candidate_message_ids": [7]},
        )
    )

    assert model_runtime.calls[1].messages[-1]["content"][0]["text"] == (
        "Custom reply repair prompt."
    )


@pytest.mark.asyncio
async def test_reply_decision_runner_fails_after_toolless_repair() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(["raw text", "still raw"])
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}],
            metadata={"candidate_message_ids": [7]},
        )
    )

    assert result.replied is False
    assert result.target_message_ids == [7]
    assert result.reason == "llm_reply_decision_toolless_after_repair"
    assert len(model_runtime.calls) == 2
    assert tool_manager.execute_calls == []


@pytest.mark.asyncio
async def test_review_llm_runner_uses_configured_prompt_components() -> None:
    prompt_registry = PromptRegistry()
    prompt_registry.register_component(
        PromptComponent(
            id="review.scan.system",
            stage=PromptStage.SYSTEM_BASE,
            kind=PromptComponentKind.STATIC_TEXT,
            content="registered review system",
        )
    )
    prompt_registry.register_component(
        PromptComponent(
            id="review.scan.contract",
            stage=PromptStage.CONSTRAINTS,
            kind=PromptComponentKind.STATIC_TEXT,
            content="registered output contract",
        )
    )
    model_runtime = FakeModelRuntime(['{"candidate_message_ids": [8], "reason": "selected"}'])
    runner = LLMReviewScanStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(
            component_ids_by_stage={
                PromptStage.SYSTEM_BASE: ["review.scan.system"],
                PromptStage.CONSTRAINTS: ["review.scan.contract"],
            },
        ),
        prompt_registry=prompt_registry,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="review_scan",
            source_messages=[{"id": 8, "raw_text": "hello"}],
        )
    )

    assert result.candidate_message_ids == [8]
    call = model_runtime.calls[0]
    message_text = "\n".join(
        block["text"]
        for message in call.messages
        for block in message["content"]
        if isinstance(block, dict) and "text" in block
    )
    assert "registered review system" in message_text
    assert "registered output contract" in message_text


@pytest.mark.asyncio
async def test_review_llm_runner_uses_registered_builtin_review_prompts() -> None:
    prompt_registry = _make_prompt_registry()
    model_runtime = FakeModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "no_reply",
                            "arguments": "{}",
                        },
                    }
                ]
            }
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(),
        prompt_registry=prompt_registry,
        tool_manager=FakeReviewToolManager(),
    )

    await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}],
            metadata={"candidate_message_ids": [7]},
        )
    )

    message_text = "\n".join(
        block["text"]
        for message in model_runtime.calls[0].messages
        for block in message["content"]
        if isinstance(block, dict) and "text" in block
    )
    assert "第一个 send_reply 必须包含 quote_message_log_id" in message_text
    assert "candidate_message_ids 是回复考虑的核心消息" in message_text
    assert "裸助手文本是无效的" in message_text
    assert "send_poke 是可选" in message_text


@pytest.mark.asyncio
async def test_review_llm_runner_uses_registered_system_prompt() -> None:
    prompt_registry = _make_prompt_registry()
    model_runtime = FakeModelRuntime(
        ['{"candidate_message_ids": [7], "reason": "selected"}']
    )
    runner = LLMReviewScanStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(),
        prompt_registry=prompt_registry,
    )

    await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="review_scan",
            source_messages=[{"id": 7, "raw_text": "hello"}],
        )
    )

    message_text = "\n".join(
        block["text"]
        for message in model_runtime.calls[0].messages
        for block in message["content"]
        if isinstance(block, dict) and "text" in block
    )
    assert "ShinBot Agent 审查流程的内部阶段" in message_text
    assert "message_log id" in message_text


@pytest.mark.asyncio
async def test_review_runner_factory_uses_llm_stages_by_default() -> None:
    model_runtime = FakeModelRuntime(
        [
            '{"candidate_message_ids": [9], "reason": "selected"}',
            '{"disposition": "watch", "reason": "observe"}',
        ]
    )
    factory = ReviewRunnerFactory(
        model_runtime,
        prompt_registry=_make_prompt_registry(),
    )
    stage_input = ReviewStageInput(
        session_id="bot:group:room",
        purpose="review_scan",
        source_messages=[{"id": 1}],
    )

    scan = await factory.create_review_scan_runner().run(stage_input)
    bootstrap = await factory.create_active_chat_bootstrap_runner().run(stage_input)
    workflow_kwargs = factory.create_workflow_runner_kwargs()

    assert scan.candidate_message_ids == [9]
    assert bootstrap.disposition == ActiveChatDisposition.WATCH
    assert set(workflow_kwargs) == {
        "compression_runner",
        "scan_runner",
        "block_digest_runner",
        "reply_runner",
        "bootstrap_runner",
    }
    assert len(model_runtime.calls) == 2


@pytest.mark.asyncio
async def test_review_runner_factory_keeps_explicitly_disabled_stages_noop() -> None:
    model_runtime = FakeModelRuntime(
        ['{"candidate_message_ids": [9], "reason": "should_not_run"}']
    )
    factory = ReviewRunnerFactory(
        model_runtime,
        config=ReviewRuntimeConfig(
            review_scan=ReviewStageRuntimeConfig(enabled=False),
            active_chat_bootstrap=ReviewStageRuntimeConfig(enabled=False),
        ),
        prompt_registry=_make_prompt_registry(),
    )
    stage_input = ReviewStageInput(
        session_id="bot:group:room",
        purpose="review_scan",
        source_messages=[{"id": 1}],
    )

    scan = await factory.create_review_scan_runner().run(stage_input)
    bootstrap = await factory.create_active_chat_bootstrap_runner().run(stage_input)

    assert scan.candidate_message_ids == []
    assert bootstrap.disposition is None
    assert model_runtime.calls == []


@pytest.mark.asyncio
async def test_review_runner_factory_builds_enabled_llm_stage() -> None:
    model_runtime = FakeModelRuntime(['{"candidate_message_ids": [9], "reason": "selected"}'])
    prompt_registry = _make_prompt_registry()
    prompt_registry.register_component(
        PromptComponent(
            id="review.scan.contract",
            stage=PromptStage.CONSTRAINTS,
            kind=PromptComponentKind.STATIC_TEXT,
            content="return candidate ids",
        )
    )
    factory = ReviewRunnerFactory(
        model_runtime,
        config=ReviewRuntimeConfig(
            review_scan=ReviewStageRuntimeConfig(
                enabled=True,
                route_id="route-a",
                model_id="model-a",
                caller="test.review",
                component_ids_by_stage={
                    PromptStage.CONSTRAINTS: ["review.scan.contract"],
                },
                params={"temperature": 0},
            ),
            reply_decision=ReviewStageRuntimeConfig(enabled=False),
            overflow_compression=ReviewStageRuntimeConfig(enabled=False),
            active_chat_bootstrap=ReviewStageRuntimeConfig(enabled=False),
        ),
        prompt_registry=prompt_registry,
    )
    stage_input = ReviewStageInput(
        session_id="bot:group:room",
        purpose="review_scan",
        source_messages=[{"id": 1}],
    )

    scan = await factory.create_review_scan_runner().run(stage_input)

    assert scan.candidate_message_ids == [9]
    assert scan.reason == "selected"
    assert model_runtime.calls[0].route_id == "route-a"
    assert model_runtime.calls[0].model_id == "model-a"
    assert model_runtime.calls[0].caller == "test.review"
    assert model_runtime.calls[0].params == {"temperature": 0}


def test_review_runtime_config_loads_plain_mapping() -> None:
    config = ReviewRuntimeConfig.from_mapping(
        {
            "review_scan": {
                "enabled": True,
                "route_id": "route-a",
                "model_id": "model-a",
                "caller": "custom.review",
                "profile_id": "review.profile",
                "component_ids_by_stage": {
                    "system_base": ["review.system"],
                    "constraints": "review.contract",
                    "unknown": ["ignored"],
                },
                "params": {"temperature": 0},
            },
            "reply_decision": {
                "special_prompt_ids": {"repair": "custom.reply.repair"},
            },
            "active_chat_bootstrap": {"enabled": False, "params": "ignored"},
        }
    )

    assert config.review_scan.enabled is True
    assert config.review_scan.route_id == "route-a"
    assert config.review_scan.model_id == "model-a"
    assert config.review_scan.caller == "custom.review"
    assert config.review_scan.profile_id == "review.profile"
    assert config.review_scan.component_ids_by_stage == {
        PromptStage.SYSTEM_BASE: ["review.system"],
        PromptStage.CONSTRAINTS: ["review.contract"],
    }
    assert config.review_scan.params == {"temperature": 0}
    assert config.reply_decision.enabled is True
    assert config.reply_decision.special_prompt_ids == {"repair": "custom.reply.repair"}
    assert config.active_chat_bootstrap.enabled is False
    assert config.active_chat_bootstrap.params == {}


@pytest.mark.asyncio
async def test_review_workflow_records_overflow_plan_and_enters_active_chat() -> None:
    scheduler = FakeReviewScheduler()
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(
            review_scan_batch_size=2,
            overflow_threshold_messages=3,
            provisional_active_chat_interest=15.0,
            provisional_active_chat_half_life_seconds=20.0,
        ),
        now=lambda: 100.0,
    )

    result = await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=ReviewPlan(
            session_id="bot:group:room",
            next_review_at=100.0,
            reason="test_review",
        ),
        unread_messages=[],
    )

    assert result.failed is False
    assert result.scan.scanned_message_count == 3
    assert result.scan.loaded_message_count == 0
    assert result.scan.batch_count == 2
    assert len(result.scan.compressed_ranges) == 1
    assert result.scan.compressed_ranges[0].start_msg_log_id == 1
    assert result.scan.compressed_ranges[0].end_msg_log_id == 2
    assert result.scan.compressed_ranges[0].message_count == 2
    assert result.reply.target_message_ids == []
    assert result.bootstrap.disposition is None
    assert result.bootstrap.reason == "active_chat_bootstrap_scheduled"
    assert result.bootstrap.tail_history_start_at == -80_000.0
    assert result.bootstrap.tail_history_end_at is None
    assert result.consumed_range_ids == []
    await workflow.wait_pending_bootstraps()
    completed_bootstrap = workflow.last_bootstrap_result("bot:group:room")
    assert completed_bootstrap is not None
    assert completed_bootstrap.reason == "active_chat_bootstrap_skipped_no_message_store"
    assert completed_bootstrap.tail_history_end_at == 100_000.0
    assert scheduler.complete_review_calls == [
        {
            "session_id": "bot:group:room",
            "enter_active_chat": True,
            "active_chat_initial_interest": 15.0,
            "active_chat_decay_half_life_seconds": 20.0,
            "next_review_plan": None,
            "now": None,
        }
    ]


@pytest.mark.asyncio
async def test_review_workflow_uses_message_store_for_scan_and_tail_history(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    message_ids = [
        _insert_message(db, raw_text=f"m{index}", created_at=float(index * 1000))
        for index in range(1, 6)
    ]
    for message_id in message_ids:
        db.agent_scheduler.add_unread(
            UnreadMessage(
                session_id="bot:group:room",
                message_log_id=message_id,
                sender_id="user-1",
                created_at=float(message_id),
            )
        )
    review_plan = FixedReviewPolicy().initial_plan(session_id="bot:group:room", now=10.0)
    db.agent_scheduler.set_review_plan(review_plan)
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        inbox=db.agent_scheduler,
        state_store=db.agent_scheduler,
        now=lambda: 10.0,
    )
    scheduler.prepare_due_review("bot:group:room", now=10.0)
    context_builder = RecordingReviewContextBuilder()
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(review_scan_batch_size=2),
        message_store=DatabaseReviewMessageStore(db),
        context_builder=context_builder,
        now=lambda: 5.0,
    )

    result = await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=scheduler.unread_messages("bot:group:room"),
    )

    assert result.scan.scanned_message_count == 5
    assert result.scan.loaded_message_count == 5
    assert result.scan.stage_input_count == 3
    assert result.scan.batch_count == 3
    assert result.bootstrap.reason == "active_chat_bootstrap_scheduled"
    assert result.consumed_range_ids == [1]
    assert [(item.start_msg_log_id, item.end_msg_log_id, item.full_range) for item in result.consumed_ranges] == [
        (message_ids[0], message_ids[-1], True)
    ]
    assert [trace.purpose for trace in result.stage_traces] == [
        "review_scan",
        "review_scan",
        "review_scan",
    ]
    assert result.stage_traces[0].message_ids == message_ids[:2]
    assert scheduler.unread_messages("bot:group:room") == []
    assert scheduler.state_for("bot:group:room") == AgentState.ACTIVE_CHAT
    await workflow.wait_pending_bootstraps()
    completed_bootstrap = workflow.last_bootstrap_result("bot:group:room")
    assert completed_bootstrap is not None
    assert completed_bootstrap.tail_history_message_count == 5
    assert completed_bootstrap.stage_input_built is True
    assert [call["purpose"] for call in context_builder.calls] == [
        "review_scan",
        "review_block_digest",
        "review_scan",
        "review_block_digest",
        "review_scan",
        "review_block_digest",
        "active_chat_bootstrap",
    ]


@pytest.mark.asyncio
async def test_review_workflow_freezes_unread_snapshot_at_entry(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    first_message_id = _insert_message(db, raw_text="before review", created_at=1000.0)
    db.agent_scheduler.add_unread(
        UnreadMessage(
            session_id="bot:group:room",
            message_log_id=first_message_id,
            sender_id="user-1",
            created_at=1.0,
        )
    )
    review_plan = FixedReviewPolicy().initial_plan(session_id="bot:group:room", now=10.0)
    db.agent_scheduler.set_review_plan(review_plan)
    now = 10.0
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        inbox=db.agent_scheduler,
        state_store=db.agent_scheduler,
        now=lambda: now,
    )
    scheduler.prepare_due_review("bot:group:room", now=10.0)
    frozen_unread = scheduler.unread_messages("bot:group:room")

    second_message_id = _insert_message(db, raw_text="during review", created_at=2000.0)
    now = 11.0
    await scheduler.accept_signal(
        AgentEntrySignal(
            session_id="bot:group:room",
            message_log_id=second_message_id,
            event_type="message-created",
            sender_id="user-2",
            instance_id="bot",
            platform="mock",
            self_id="bot-self",
            is_private=False,
            is_mentioned=False,
            is_reply_to_bot=False,
        )
    )

    context_builder = RecordingReviewContextBuilder()
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(review_scan_batch_size=10),
        message_store=DatabaseReviewMessageStore(db),
        context_builder=context_builder,
        now=lambda: 12.0,
    )

    result = await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=frozen_unread,
    )

    assert result.scan.scanned_message_count == 1
    assert [trace.message_ids for trace in result.stage_traces if trace.purpose == "review_scan"] == [
        [first_message_id]
    ]
    assert [(item.start_msg_log_id, item.end_msg_log_id, item.full_range) for item in result.consumed_ranges] == [
        (first_message_id, first_message_id, False)
    ]
    assert [message.message_log_id for message in scheduler.unread_messages("bot:group:room")] == [
        second_message_id
    ]
    assert scheduler.state_for("bot:group:room") == AgentState.ACTIVE_CHAT


@pytest.mark.asyncio
async def test_concurrent_review_runs_keep_distinct_review_run_ids(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    sessions = ["bot:group:room-a", "bot:group:room-b"]
    message_ids_by_session: dict[str, list[int]] = {}
    for session_index, session_id in enumerate(sessions):
        message_ids = [
            _insert_message(
                db,
                session_id=session_id,
                raw_text=f"{session_id}-m{index}",
                created_at=float((session_index + 1) * 10_000 + index * 1000),
            )
            for index in range(1, 3)
        ]
        message_ids_by_session[session_id] = message_ids
        for message_id in message_ids:
            db.agent_scheduler.add_unread(
                UnreadMessage(
                    session_id=session_id,
                    message_log_id=message_id,
                    sender_id="user-1",
                    created_at=float(message_id),
                )
            )
        db.agent_scheduler.set_review_plan(
            FixedReviewPolicy().initial_plan(session_id=session_id, now=10.0)
        )
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        inbox=db.agent_scheduler,
        state_store=db.agent_scheduler,
        now=lambda: 10.0,
    )
    review_inputs = []
    for session_id in sessions:
        decision = scheduler.prepare_due_review(session_id, now=10.0)
        assert decision.review_plan is not None
        review_inputs.append((
            session_id,
            decision.review_plan,
            scheduler.unread_messages(session_id),
        ))
    context_builder = RecordingReviewContextBuilder()
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(review_scan_batch_size=2),
        message_store=DatabaseReviewMessageStore(db),
        context_builder=context_builder,
        scan_runner=YieldingReviewScanRunner(),
        reply_runner=RecordingReplyDecisionRunner(),
        now=lambda: 5.0,
    )

    results = await asyncio.gather(*[
        workflow.run(
            scheduler=scheduler,
            session_id=session_id,
            review_plan=review_plan,
            unread_messages=unread_messages,
        )
        for session_id, review_plan, unread_messages in review_inputs
    ])
    await workflow.wait_pending_bootstraps()

    run_id_by_session = {result.completion.session_id: result.review_run_id for result in results if result.completion is not None}
    assert len(set(run_id_by_session.values())) == 2
    for call in context_builder.calls:
        assert call["metadata"]["review_run_id"] == run_id_by_session[call["session_id"]]


@pytest.mark.asyncio
async def test_review_scan_runner_selects_candidate_message_ids(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    message_ids = [
        _insert_message(db, raw_text=f"m{index}", created_at=float(index * 1000))
        for index in range(1, 5)
    ]
    for message_id in message_ids:
        db.agent_scheduler.add_unread(
            UnreadMessage(
                session_id="bot:group:room",
                message_log_id=message_id,
                sender_id="user-1",
                created_at=float(message_id),
            )
        )
    review_plan = FixedReviewPolicy().initial_plan(session_id="bot:group:room", now=10.0)
    db.agent_scheduler.set_review_plan(review_plan)
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        inbox=db.agent_scheduler,
        state_store=db.agent_scheduler,
        now=lambda: 10.0,
    )
    scheduler.prepare_due_review("bot:group:room", now=10.0)
    scan_runner = SelectingReviewScanRunner()
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(review_scan_batch_size=2),
        message_store=DatabaseReviewMessageStore(db),
        context_builder=RecordingReviewContextBuilder(),
        scan_runner=scan_runner,
        now=lambda: 5.0,
    )

    result = await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=scheduler.unread_messages("bot:group:room"),
    )

    assert result.scan.candidate_message_ids == [message_ids[1], message_ids[3]]
    assert result.scan.scan_reason == "selected_from_2"
    assert result.reply.target_message_ids == [message_ids[1], message_ids[3]]
    assert [call["purpose"] for call in scan_runner.calls] == ["review_scan", "review_scan"]
    assert [call["message_ids"] for call in scan_runner.calls] == [
        message_ids[:2],
        message_ids[2:],
    ]


@pytest.mark.asyncio
async def test_reply_decision_runner_reads_candidate_local_context(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    message_ids = [
        _insert_message(db, raw_text=f"m{index}", created_at=float(index * 1000))
        for index in range(1, 6)
    ]
    for message_id in message_ids:
        db.agent_scheduler.add_unread(
            UnreadMessage(
                session_id="bot:group:room",
                message_log_id=message_id,
                sender_id="user-1",
                created_at=float(message_id),
            )
        )
    review_plan = FixedReviewPolicy().initial_plan(session_id="bot:group:room", now=10.0)
    db.agent_scheduler.set_review_plan(review_plan)
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        inbox=db.agent_scheduler,
        state_store=db.agent_scheduler,
        now=lambda: 10.0,
    )
    scheduler.prepare_due_review("bot:group:room", now=10.0)
    scan_runner = SelectingReviewScanRunner()
    reply_runner = RecordingReplyDecisionRunner()
    context_builder = RecordingReviewContextBuilder()
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(
            review_scan_batch_size=5,
            reply_context_before_messages=1,
            reply_context_after_messages=1,
        ),
        message_store=DatabaseReviewMessageStore(db),
        context_builder=context_builder,
        scan_runner=scan_runner,
        reply_runner=reply_runner,
        now=lambda: 5.0,
    )

    result = await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=scheduler.unread_messages("bot:group:room"),
    )

    assert result.scan.candidate_message_ids == [message_ids[-1]]
    assert result.reply.target_message_ids == [message_ids[-1]]
    assert result.reply.loaded_message_count == 2
    assert result.reply.stage_input_count == 1
    assert result.reply.reply_reason == f"checked_{message_ids[-1]}"
    assert _strip_run_id_from_calls(reply_runner.calls) == [
        {
            "purpose": "reply_decision",
            "candidate_id": message_ids[-1],
            "message_ids": message_ids[-2:],
            "metadata": {
                "purpose": "reply_decision",
                "candidate_message_id": message_ids[-1],
                "candidate_message_ids": [message_ids[-1]],
                "before_messages": 1,
                "after_messages": 1,
            },
        }
    ]
    assert [call["purpose"] for call in context_builder.calls] == [
        "review_scan",
        "review_block_digest",
        "reply_decision",
    ]
    await workflow.wait_pending_bootstraps()
    assert [call["purpose"] for call in context_builder.calls] == [
        "review_scan",
        "review_block_digest",
        "reply_decision",
        "active_chat_bootstrap",
    ]


@pytest.mark.asyncio
async def test_reply_decision_groups_overlapping_candidate_contexts(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    message_ids = [
        _insert_message(db, raw_text=f"m{index}", created_at=float(index * 1000))
        for index in range(1, 8)
    ]
    for message_id in message_ids:
        db.agent_scheduler.add_unread(
            UnreadMessage(
                session_id="bot:group:room",
                message_log_id=message_id,
                sender_id="user-1",
                created_at=float(message_id),
            )
        )
    review_plan = FixedReviewPolicy().initial_plan(session_id="bot:group:room", now=10.0)
    db.agent_scheduler.set_review_plan(review_plan)
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        inbox=db.agent_scheduler,
        state_store=db.agent_scheduler,
        now=lambda: 10.0,
    )
    scheduler.prepare_due_review("bot:group:room", now=10.0)
    reply_runner = RecordingReplyDecisionRunner()
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(
            review_scan_batch_size=7,
            reply_context_before_messages=1,
            reply_context_after_messages=1,
        ),
        message_store=DatabaseReviewMessageStore(db),
        context_builder=RecordingReviewContextBuilder(),
        scan_runner=FixedCandidateScanRunner([message_ids[2], message_ids[3]]),
        reply_runner=reply_runner,
        now=lambda: 5.0,
    )

    result = await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=scheduler.unread_messages("bot:group:room"),
    )

    assert result.scan.candidate_message_ids == [message_ids[2], message_ids[3]]
    assert result.reply.target_message_ids == [message_ids[2], message_ids[3]]
    assert result.reply.stage_input_count == 1
    assert result.reply.loaded_message_count == 4
    assert _strip_run_id_from_calls(reply_runner.calls) == [
        {
            "purpose": "reply_decision",
            "candidate_id": message_ids[2],
            "message_ids": message_ids[1:5],
            "metadata": {
                "purpose": "reply_decision",
                "candidate_message_id": message_ids[2],
                "candidate_message_ids": [message_ids[2], message_ids[3]],
                "before_messages": 1,
                "after_messages": 1,
            },
        }
    ]
    assert result.stage_traces[1].metadata["candidate_message_ids"] == [
        message_ids[2],
        message_ids[3],
    ]


@pytest.mark.asyncio
async def test_reply_decision_receives_target_and_adjacent_block_digests(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    message_ids = [
        _insert_message(db, raw_text=f"m{index}", created_at=float(index * 1000))
        for index in range(1, 7)
    ]
    for message_id in message_ids:
        db.agent_scheduler.add_unread(
            UnreadMessage(
                session_id="bot:group:room",
                message_log_id=message_id,
                sender_id="user-1",
                created_at=float(message_id),
            )
        )
    review_plan = FixedReviewPolicy().initial_plan(session_id="bot:group:room", now=10.0)
    db.agent_scheduler.set_review_plan(review_plan)
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        inbox=db.agent_scheduler,
        state_store=db.agent_scheduler,
        now=lambda: 10.0,
    )
    scheduler.prepare_due_review("bot:group:room", now=10.0)
    reply_runner = RecordingReplyDecisionRunner()
    block_digest_runner = RecordingBlockDigestRunner()
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(
            review_scan_batch_size=2,
            reply_context_before_messages=0,
            reply_context_after_messages=0,
        ),
        message_store=DatabaseReviewMessageStore(db),
        context_builder=RecordingReviewContextBuilder(),
        scan_runner=FixedCandidateScanRunner([message_ids[-1]]),
        block_digest_runner=block_digest_runner,
        reply_runner=reply_runner,
        now=lambda: 5.0,
    )

    await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=scheduler.unread_messages("bot:group:room"),
    )

    assert [call["message_ids"] for call in block_digest_runner.calls] == [
        message_ids[:2],
        message_ids[2:4],
        message_ids[4:],
    ]
    block_digests = reply_runner.calls[0]["metadata"]["block_digests"]
    assert [item["block_index"] for item in block_digests] == [1, 2]
    assert [item["msg_log_start"] for item in block_digests] == [
        message_ids[2],
        message_ids[4],
    ]
    assert "digest_0" not in reply_runner.calls[0]["metadata"]["previous_summary"]
    assert "digest_1" in reply_runner.calls[0]["metadata"]["previous_summary"]
    assert "digest_2" in reply_runner.calls[0]["metadata"]["previous_summary"]


@pytest.mark.asyncio
async def test_block_digest_runner_concurrency_is_bounded(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    message_ids = [
        _insert_message(db, raw_text=f"m{index}", created_at=float(index * 1000))
        for index in range(1, 7)
    ]
    for message_id in message_ids:
        db.agent_scheduler.add_unread(
            UnreadMessage(
                session_id="bot:group:room",
                message_log_id=message_id,
                sender_id="user-1",
                created_at=float(message_id),
            )
        )
    review_plan = FixedReviewPolicy().initial_plan(session_id="bot:group:room", now=10.0)
    db.agent_scheduler.set_review_plan(review_plan)
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        inbox=db.agent_scheduler,
        state_store=db.agent_scheduler,
        now=lambda: 10.0,
    )
    scheduler.prepare_due_review("bot:group:room", now=10.0)
    block_digest_runner = SlowBlockDigestRunner()
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(
            review_scan_batch_size=1,
            review_block_digest_concurrency=2,
        ),
        message_store=DatabaseReviewMessageStore(db),
        context_builder=RecordingReviewContextBuilder(),
        scan_runner=FixedCandidateScanRunner([]),
        block_digest_runner=block_digest_runner,
        now=lambda: 5.0,
    )

    await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=scheduler.unread_messages("bot:group:room"),
    )

    assert block_digest_runner.max_active_count <= 2


@pytest.mark.asyncio
async def test_block_digest_failure_does_not_block_scan_or_reply(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    message_ids = [
        _insert_message(db, raw_text=f"m{index}", created_at=float(index * 1000))
        for index in range(1, 4)
    ]
    for message_id in message_ids:
        db.agent_scheduler.add_unread(
            UnreadMessage(
                session_id="bot:group:room",
                message_log_id=message_id,
                sender_id="user-1",
                created_at=float(message_id),
            )
        )
    review_plan = FixedReviewPolicy().initial_plan(session_id="bot:group:room", now=10.0)
    db.agent_scheduler.set_review_plan(review_plan)
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        inbox=db.agent_scheduler,
        state_store=db.agent_scheduler,
        now=lambda: 10.0,
    )
    scheduler.prepare_due_review("bot:group:room", now=10.0)
    reply_runner = RecordingReplyDecisionRunner()
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(review_scan_batch_size=3),
        message_store=DatabaseReviewMessageStore(db),
        context_builder=RecordingReviewContextBuilder(),
        scan_runner=FixedCandidateScanRunner([message_ids[-1]]),
        block_digest_runner=FailingBlockDigestRunner(),
        reply_runner=reply_runner,
        now=lambda: 5.0,
    )

    result = await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=scheduler.unread_messages("bot:group:room"),
    )

    assert result.failed is False
    assert result.scan.candidate_message_ids == [message_ids[-1]]
    assert result.reply.target_message_ids == [message_ids[-1]]
    assert "block_digests" not in reply_runner.calls[0]["metadata"]


@pytest.mark.asyncio
async def test_reply_decision_uses_latest_active_chat_summary(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    message_id = _insert_message(db, raw_text="m1", created_at=1000.0)
    db.agent_scheduler.add_unread(
        UnreadMessage(
            session_id="bot:group:room",
            message_log_id=message_id,
            sender_id="user-1",
            created_at=float(message_id),
        )
    )
    review_plan = FixedReviewPolicy().initial_plan(session_id="bot:group:room", now=10.0)
    db.agent_scheduler.set_review_plan(review_plan)
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        inbox=db.agent_scheduler,
        state_store=db.agent_scheduler,
        now=lambda: 10.0,
    )
    scheduler.prepare_due_review("bot:group:room", now=10.0)
    summary_service = FakeSummaryService()
    summary_service.session_summaries = [
        type("Summary", (), {"created_at": 1.0, "content": "old active summary"})(),
        type("Summary", (), {"created_at": 4.0, "content": "new active summary"})(),
    ]
    reply_runner = RecordingReplyDecisionRunner()
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(
            review_scan_batch_size=1,
            active_chat_summary_max_age_seconds=10,
        ),
        message_store=DatabaseReviewMessageStore(db),
        summary_service=summary_service,
        context_builder=RecordingReviewContextBuilder(),
        scan_runner=FixedCandidateScanRunner([message_id]),
        reply_runner=reply_runner,
        now=lambda: 5.0,
    )

    await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=scheduler.unread_messages("bot:group:room"),
    )

    metadata = reply_runner.calls[0]["metadata"]
    assert metadata["active_chat_summary"] == "new active summary"
    assert "new active summary" in metadata["previous_summary"]
    assert "old active summary" not in metadata["previous_summary"]


@pytest.mark.asyncio
async def test_active_chat_bootstrap_runner_receives_tail_history_and_reply_facts(
    tmp_path,
) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    message_ids = [
        _insert_message(db, raw_text=f"m{index}", created_at=float(index * 1000))
        for index in range(1, 5)
    ]
    for message_id in message_ids:
        db.agent_scheduler.add_unread(
            UnreadMessage(
                session_id="bot:group:room",
                message_log_id=message_id,
                sender_id="user-1",
                created_at=float(message_id),
            )
        )
    review_plan = FixedReviewPolicy().initial_plan(session_id="bot:group:room", now=10.0)
    db.agent_scheduler.set_review_plan(review_plan)
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        inbox=db.agent_scheduler,
        state_store=db.agent_scheduler,
        now=lambda: 10.0,
    )
    scheduler.prepare_due_review("bot:group:room", now=10.0)
    bootstrap_runner = RecordingActiveChatBootstrapRunner()
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(
            review_scan_batch_size=4,
            reply_context_before_messages=1,
            reply_context_after_messages=1,
            tail_history_before_seconds=10.0,
        ),
        message_store=DatabaseReviewMessageStore(db),
        context_builder=RecordingReviewContextBuilder(),
        scan_runner=SelectingReviewScanRunner(),
        reply_runner=RecordingReplyDecisionRunner(),
        bootstrap_runner=bootstrap_runner,
        now=lambda: 4.0,
    )

    result = await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=scheduler.unread_messages("bot:group:room"),
    )

    assert result.bootstrap.reason == "active_chat_bootstrap_scheduled"
    assert result.completion.active_chat_state.interest_value == 15.0
    assert result.completion.active_chat_state.decay_half_life_seconds == 20.0
    await workflow.wait_pending_bootstraps()
    completed_bootstrap = workflow.last_bootstrap_result("bot:group:room")
    assert completed_bootstrap is not None
    assert completed_bootstrap.disposition == ActiveChatDisposition.ENGAGED
    assert completed_bootstrap.bootstrap_applied is True
    assert completed_bootstrap.active_chat_interest_value == 40.0
    assert completed_bootstrap.active_chat_decay_half_life_seconds == 35.0
    assert completed_bootstrap.reason == "bootstrap_selected_interest"
    assert _strip_run_id_from_calls(bootstrap_runner.calls) == [
        {
            "purpose": "active_chat_bootstrap",
            "message_ids": message_ids,
            "metadata": {
                "purpose": "active_chat_bootstrap",
                "tail_history_start_at": -6000.0,
                "tail_history_end_at": 4000.0,
                "reply_replied": False,
                "reply_message_id": None,
                "reply_message_ids": [],
                "reply_target_message_ids": [message_ids[-1]],
                "reply_reason": f"checked_{message_ids[-1]}",
            },
        }
    ]


@pytest.mark.asyncio
async def test_attention_dispatcher_can_run_review_workflow() -> None:
    workflow = ReviewCoordinator(now=lambda: 100.0)
    active_chat_workflow = ActiveChatCoordinator(now=lambda: 100.0)
    dispatcher = ActiveReplyDispatcher(
        review_coordinator=workflow,
        active_chat_workflow=active_chat_workflow,
    )
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        now=lambda: 10.0,
    )

    await scheduler.accept_signal(
        AgentEntrySignal(
            session_id="bot:group:room",
            message_log_id=1,
            event_type="message-created",
            sender_id="user-1",
            instance_id="bot",
            platform="mock",
            self_id="bot-self",
            is_private=False,
            is_mentioned=False,
            is_reply_to_bot=False,
        )
    )

    decision = await scheduler.run_due_review("bot:group:room", now=10.0)

    assert decision.review_started is True
    assert decision.review_workflow_started is True
    assert decision.state == AgentState.ACTIVE_CHAT
    assert scheduler.state_for("bot:group:room") == AgentState.ACTIVE_CHAT
    active_chat_state = scheduler.active_chat_state_for("bot:group:room")
    assert active_chat_state is not None
    assert active_chat_state.interest_value == 15.0
    assert dispatcher.last_review_result is not None
    assert dispatcher.last_review_explanation is not None
    assert dispatcher.last_review_explanation.active_chat_initial_interest is None
    assert dispatcher.last_review_explanation.replied is False
    active_attention_state = active_chat_workflow.attention_state_for("bot:group:room")
    assert active_attention_state is not None
    from shinbot.agent.services.summaries import ReviewHandoffContext

    assert isinstance(active_attention_state.review_result_summary, ReviewHandoffContext)
    assert active_attention_state.review_result_summary.explanation == dispatcher.last_review_explanation


@pytest.mark.asyncio
async def test_attention_dispatcher_feeds_review_added_unread_to_active_chat(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    before_review_id = _insert_message(db, raw_text="before review", created_at=1000.0)
    during_review_id = _insert_message(db, raw_text="during review", created_at=2000.0)
    review_plan = FixedReviewPolicy().initial_plan(session_id="bot:group:room", now=10.0)
    db.agent_scheduler.set_review_plan(review_plan)

    active_chat_workflow = ActiveChatCoordinator(now=lambda: 100.0)
    dispatcher = ActiveReplyDispatcher(
        review_coordinator=ReviewCoordinator(
            ReviewWorkflowConfig(review_scan_batch_size=10),
            message_store=DatabaseReviewMessageStore(db),
            context_builder=RecordingReviewContextBuilder(),
            now=lambda: 100.0,
        ),
        active_chat_workflow=active_chat_workflow,
    )
    now = 10.0
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda signal: f"profile-{signal.message_log_id}",
        review_policy=FixedReviewPolicy(),
        inbox=db.agent_scheduler,
        state_store=db.agent_scheduler,
        now=lambda: now,
    )
    await scheduler.accept_signal(
        AgentEntrySignal(
            session_id="bot:group:room",
            message_log_id=before_review_id,
            event_type="message-created",
            sender_id="user-1",
            instance_id="bot",
            platform="mock",
            self_id="bot-self",
            is_private=False,
            is_mentioned=False,
            is_reply_to_bot=False,
        )
    )
    scheduler.prepare_due_review("bot:group:room", now=10.0)
    frozen_unread = scheduler.unread_messages("bot:group:room")

    now = 11.0
    await scheduler.accept_signal(
        AgentEntrySignal(
            session_id="bot:group:room",
            message_log_id=during_review_id,
            event_type="message-created",
            sender_id="user-2",
            instance_id="bot",
            platform="mock",
            self_id="bot-self",
            is_private=False,
            is_mentioned=False,
            is_reply_to_bot=False,
            is_mention_to_other=True,
        )
    )

    await dispatcher.run_review(
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=frozen_unread,
    )

    active_attention_state = active_chat_workflow.attention_state_for("bot:group:room")
    assert active_attention_state is not None
    assert [
        message.message_log_id
        for message in active_attention_state.pending_buffer
    ] == [during_review_id]
    seeded_signal = active_attention_state.pending_buffer[0]
    assert seeded_signal.response_profile == f"profile-{during_review_id}"
    assert seeded_signal.is_mention_to_other is True
    assert active_attention_state.accumulated == 0.5
    assert [message.message_log_id for message in scheduler.unread_messages("bot:group:room")] == [
        during_review_id
    ]
    assert dispatcher.last_review_result is not None
    assert dispatcher.last_review_result.consumed_ranges[0].start_msg_log_id == before_review_id
    await active_chat_workflow.shutdown()


@pytest.mark.asyncio
async def test_review_workflow_splits_partially_consumed_overflow_range(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    message_ids = [
        _insert_message(db, raw_text=f"m{index}", created_at=float(index * 1000))
        for index in range(1, 6)
    ]
    for message_id in message_ids:
        db.agent_scheduler.add_unread(
            UnreadMessage(
                session_id="bot:group:room",
                message_log_id=message_id,
                sender_id="user-1",
                created_at=float(message_id),
            )
        )
    review_plan = FixedReviewPolicy().initial_plan(session_id="bot:group:room", now=10.0)
    db.agent_scheduler.set_review_plan(review_plan)
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        inbox=db.agent_scheduler,
        state_store=db.agent_scheduler,
        now=lambda: 10.0,
    )
    scheduler.prepare_due_review("bot:group:room", now=10.0)
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(
            review_scan_batch_size=10,
            overflow_threshold_messages=3,
        ),
        message_store=DatabaseReviewMessageStore(db),
        context_builder=RecordingReviewContextBuilder(),
        now=lambda: 5.0,
    )

    result = await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=scheduler.unread_messages("bot:group:room"),
    )

    assert result.scan.scanned_message_count == 3
    assert [(item.start_msg_log_id, item.end_msg_log_id, item.full_range) for item in result.consumed_ranges] == [
        (message_ids[2], message_ids[-1], False)
    ]
    assert [message.message_log_id for message in scheduler.unread_messages("bot:group:room")] == [
        message_ids[0],
        message_ids[1],
    ]
    assert [
        (item.start_msg_log_id, item.end_msg_log_id, item.message_count)
        for item in scheduler.unread_ranges("bot:group:room")
    ] == [(message_ids[0], message_ids[1], 2)]


@pytest.mark.asyncio
async def test_overflow_compression_runner_summarizes_old_unread_prefix(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    message_ids = [
        _insert_message(db, raw_text=f"m{index}", created_at=float(index * 1000))
        for index in range(1, 6)
    ]
    for message_id in message_ids:
        db.agent_scheduler.add_unread(
            UnreadMessage(
                session_id="bot:group:room",
                message_log_id=message_id,
                sender_id="user-1",
                created_at=float(message_id),
            )
        )
    review_plan = FixedReviewPolicy().initial_plan(session_id="bot:group:room", now=10.0)
    db.agent_scheduler.set_review_plan(review_plan)
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        inbox=db.agent_scheduler,
        state_store=db.agent_scheduler,
        now=lambda: 10.0,
    )
    scheduler.prepare_due_review("bot:group:room", now=10.0)
    compression_runner = RecordingOverflowCompressionRunner()
    context_builder = RecordingReviewContextBuilder()
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(
            review_scan_batch_size=10,
            overflow_threshold_messages=3,
        ),
        message_store=DatabaseReviewMessageStore(db),
        summary_store=DatabaseReviewSummaryStore(db),
        context_builder=context_builder,
        compression_runner=compression_runner,
        now=lambda: 5.0,
    )

    result = await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=scheduler.unread_messages("bot:group:room"),
    )

    assert len(result.scan.compressed_ranges) == 1
    compressed = result.scan.compressed_ranges[0]
    assert compressed.summary == "older messages summarized"
    assert compressed.candidate_message_ids == [message_ids[0]]
    assert compressed.reason == "compressed_old_messages"
    assert result.scan.candidate_message_ids == [message_ids[0]]
    assert result.reply.target_message_ids == [message_ids[0]]
    persisted_summaries = DatabaseReviewSummaryStore(db).list_summaries("bot:group:room")
    assert len(persisted_summaries) == 1
    assert persisted_summaries[0].summary == "older messages summarized"
    assert persisted_summaries[0].candidate_message_ids == [message_ids[0]]
    assert persisted_summaries[0].reason == "compressed_old_messages"
    assert persisted_summaries[0].start_msg_log_id == message_ids[0]
    assert persisted_summaries[0].end_msg_log_id == message_ids[1]
    assert _strip_run_id_from_calls(compression_runner.calls) == [
        {
            "purpose": "overflow_compression",
            "message_ids": message_ids[:2],
            "metadata": {
                "purpose": "overflow_compression",
                "start_msg_log_id": message_ids[0],
                "end_msg_log_id": message_ids[1],
                "message_count": 2,
                "reason": "overflow_pending_compression",
            },
        }
    ]
    assert [message.message_log_id for message in scheduler.unread_messages("bot:group:room")] == [
        message_ids[0],
        message_ids[1],
    ]
    assert [call["purpose"] for call in context_builder.calls][:2] == [
        "overflow_compression",
        "review_scan",
    ]
    review_scan_call = next(
        call for call in context_builder.calls if call["purpose"] == "review_scan"
    )
    assert "older messages summarized" in review_scan_call["previous_summary"]
    assert review_scan_call["metadata"]["overflow_summaries"][0]["summary"] == (
        "older messages summarized"
    )
    reply_call = next(
        call for call in context_builder.calls if call["purpose"] == "reply_decision"
    )
    await workflow.wait_pending_bootstraps()
    bootstrap_call = next(
        call for call in context_builder.calls if call["purpose"] == "active_chat_bootstrap"
    )
    assert "older messages summarized" in reply_call["previous_summary"]
    assert "older messages summarized" in bootstrap_call["previous_summary"]
    assert [trace.purpose for trace in result.stage_traces] == [
        "overflow_compression",
        "review_scan",
        "reply_decision",
    ]
    assert result.stage_traces[0].reason == "compressed_old_messages"
    assert result.stage_traces[0].candidate_message_ids == [message_ids[0]]
    assert result.stage_traces[1].metadata["overflow_summaries"][0]["summary"] == (
        "older messages summarized"
    )
    assert "older messages summarized" in result.stage_traces[1].previous_summary


def test_review_workflow_explanation_summarizes_result() -> None:
    from shinbot.agent.coordinators.review.models import (
        ActiveChatBootstrapResult,
        ConsumedUnreadRange,
        ReplyDecisionResult,
        ReviewScanResult,
        ReviewStageTrace,
        ReviewWorkflowResult,
        UnreadRangeSummaryRecord,
    )

    result = ReviewWorkflowResult(
        review_run_id="test_run_id",
        scan=ReviewScanResult(
            candidate_message_ids=[3],
            scanned_message_count=5,
            loaded_message_count=3,
            batch_count=2,
            compressed_ranges=[
                UnreadRangeSummaryRecord(
                    session_id="bot:group:room",
                    start_msg_log_id=1,
                    end_msg_log_id=2,
                    start_at=1.0,
                    end_at=2.0,
                    message_count=2,
                    summary="older context",
                )
            ],
        ),
        reply=ReplyDecisionResult(
            replied=True,
            reply_message_id=10,
            reply_message_ids=[10],
            target_message_ids=[3],
            reply_reason="answered",
        ),
        bootstrap=ActiveChatBootstrapResult(
            disposition=ActiveChatDisposition.CASUAL,
            bootstrap_applied=True,
            active_chat_interest_value=40.0,
            active_chat_decay_half_life_seconds=30.0,
            reason="keep_chatting",
        ),
        review_started_at=100.0,
        consumed_range_ids=[7],
        consumed_ranges=[
            ConsumedUnreadRange(
                range_id=7,
                session_id="bot:group:room",
                start_msg_log_id=3,
                end_msg_log_id=5,
                message_count=3,
                full_range=True,
            )
        ],
        stage_traces=[
            ReviewStageTrace(
                purpose="reply_decision",
                message_ids=[2, 3, 4],
                reason="answered",
                target_message_ids=[3],
                replied=True,
                reply_message_id=10,
                reply_message_ids=[10],
            ),
            ReviewStageTrace(
                purpose="active_chat_bootstrap",
                message_ids=[3, 4, 5],
                reason="keep_chatting",
                active_chat_disposition=ActiveChatDisposition.CASUAL,
                active_chat_bootstrap_applied=True,
                active_chat_interest_value=40.0,
                active_chat_decay_half_life_seconds=30.0,
            ),
        ],
    )

    explanation = build_review_workflow_explanation(result)

    assert explanation.review_started_at == 100.0
    assert explanation.scanned_message_count == 5
    assert explanation.loaded_message_count == 3
    assert explanation.reviewed_batch_count == 2
    assert explanation.candidate_message_ids == [3]
    assert explanation.reply_target_message_ids == [3]
    assert explanation.replied is True
    assert explanation.reply_message_id == 10
    assert explanation.reply_message_ids == [10]
    assert explanation.overflow_summary_count == 1
    assert explanation.overflow_summary_message_count == 2
    assert explanation.consumed_range_ids == [7]
    assert explanation.consumed_message_count == 3
    assert explanation.active_chat_initial_interest == 40.0
    assert explanation.active_chat_decay_half_life_seconds == 30.0
    assert explanation.active_chat_disposition == ActiveChatDisposition.CASUAL
    assert explanation.active_chat_bootstrap_applied is True
    assert explanation.active_chat_reason == "keep_chatting"
    assert [stage.purpose for stage in explanation.stages] == [
        "reply_decision",
        "active_chat_bootstrap",
    ]
    assert explanation.stages[0].input_message_count == 3
    assert explanation.stages[0].target_message_ids == [3]
    assert explanation.stages[0].replied is True
    assert explanation.stages[0].reply_message_ids == [10]
    assert explanation.stages[1].active_chat_interest_value == 40.0
    assert explanation.stages[1].active_chat_disposition == ActiveChatDisposition.CASUAL


@pytest.mark.asyncio
async def test_review_workflow_uses_actual_message_bounds_for_interleaved_sessions(
    tmp_path,
) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    message_ids = [
        _insert_message(db, raw_text="room-1", created_at=1000.0),
        _insert_message(db, session_id="bot:group:other", raw_text="other-1", created_at=1500.0),
        _insert_message(db, raw_text="room-2", created_at=2000.0),
        _insert_message(db, session_id="bot:group:other", raw_text="other-2", created_at=2500.0),
        _insert_message(db, raw_text="room-3", created_at=3000.0),
    ]
    room_message_ids = [message_ids[0], message_ids[2], message_ids[4]]
    for message_id in room_message_ids:
        db.agent_scheduler.add_unread(
            UnreadMessage(
                session_id="bot:group:room",
                message_log_id=message_id,
                sender_id="user-1",
                created_at=float(message_id),
            )
        )
    review_plan = FixedReviewPolicy().initial_plan(session_id="bot:group:room", now=10.0)
    db.agent_scheduler.set_review_plan(review_plan)
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        inbox=db.agent_scheduler,
        state_store=db.agent_scheduler,
        now=lambda: 10.0,
    )
    scheduler.prepare_due_review("bot:group:room", now=10.0)
    compression_runner = RecordingOverflowCompressionRunner()
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(
            review_scan_batch_size=10,
            overflow_threshold_messages=1,
        ),
        message_store=DatabaseReviewMessageStore(db),
        summary_store=DatabaseReviewSummaryStore(db),
        context_builder=RecordingReviewContextBuilder(),
        compression_runner=compression_runner,
        now=lambda: 5.0,
    )

    result = await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=scheduler.unread_messages("bot:group:room"),
    )

    assert [call["message_ids"] for call in compression_runner.calls] == [
        room_message_ids[:2]
    ]
    assert result.scan.compressed_ranges[0].start_msg_log_id == room_message_ids[0]
    assert result.scan.compressed_ranges[0].end_msg_log_id == room_message_ids[1]
    assert [(item.start_msg_log_id, item.end_msg_log_id) for item in result.consumed_ranges] == [
        (room_message_ids[2], room_message_ids[2])
    ]
    assert [message.message_log_id for message in scheduler.unread_messages("bot:group:room")] == [
        room_message_ids[0],
        room_message_ids[1],
    ]


class _HandoffFakeSummaryRecord:
    def __init__(
        self,
        *,
        content: str,
        block_index: int | None = None,
        msg_log_start: int | None = None,
        msg_log_end: int | None = None,
        msg_count: int = 0,
        created_at: float = 0.0,
    ) -> None:
        self.content = content
        self.block_index = block_index
        self.msg_log_start = msg_log_start
        self.msg_log_end = msg_log_end
        self.msg_count = msg_count
        self.created_at = created_at


class _HandoffFakeSummaryService:
    def __init__(
        self,
        *,
        overflow_records: list[Any] | None = None,
        digest_records: list[Any] | None = None,
        active_record: Any | None = None,
    ) -> None:
        self._overflow_records = overflow_records or []
        self._digest_records = digest_records or []
        self._active_record = active_record
        self.list_by_run_id_calls: list[tuple[str, Any]] = []
        self.get_latest_calls: list[tuple[str, Any]] = []

    def list_by_run_id(
        self, run_id: str, *, summary_type: Any = None,
    ) -> list[Any]:
        self.list_by_run_id_calls.append((run_id, summary_type))
        from shinbot.agent.services.summaries import SummaryType

        if summary_type == SummaryType.OVERFLOW_COMPRESSION:
            return self._overflow_records
        if summary_type == SummaryType.BLOCK_DIGEST:
            return self._digest_records
        return []

    def get_latest_by_session(
        self, session_id: str, *, summary_type: Any = None,
    ) -> Any | None:
        self.get_latest_calls.append((session_id, summary_type))
        return self._active_record


@pytest.mark.asyncio
async def test_dispatcher_build_handoff_context_with_summaries() -> None:
    from shinbot.agent.coordinators.review.models import (
        ActiveChatBootstrapResult,
        ReplyDecisionResult,
        ReviewScanResult,
        ReviewWorkflowConfig,
        ReviewWorkflowExplanation,
        ReviewWorkflowResult,
    )
    from shinbot.agent.services.summaries import (
        ReviewHandoffContext,
        SummaryHandoffEntry,
        SummaryType,
    )

    explanation = ReviewWorkflowExplanation(
        review_run_id="run_abc",
        review_started_at=100.0,
    )
    result = ReviewWorkflowResult(
        review_run_id="run_abc",
        scan=ReviewScanResult(),
        reply=ReplyDecisionResult(),
        bootstrap=ActiveChatBootstrapResult(),
        review_started_at=100.0,
    )

    fake_summary_service = _HandoffFakeSummaryService(
        overflow_records=[
            _HandoffFakeSummaryRecord(
                content="Old overflow summary.",
                msg_log_start=1,
                msg_log_end=10,
                msg_count=10,
            ),
        ],
        digest_records=[
            _HandoffFakeSummaryRecord(
                content="Block 0 digest.",
                block_index=0,
                msg_log_start=11,
                msg_log_end=20,
                msg_count=10,
            ),
            _HandoffFakeSummaryRecord(
                content="Block 1 digest.",
                block_index=1,
                msg_log_start=21,
                msg_log_end=30,
                msg_count=10,
            ),
        ],
        active_record=_HandoffFakeSummaryRecord(
            content="Previous active chat context.",
            created_at=9999999999.0,
        ),
    )
    dispatcher = ActiveReplyDispatcher(
        summary_service=fake_summary_service,
        review_config=ReviewWorkflowConfig(active_chat_summary_max_age_seconds=1800.0),
    )

    handoff = await dispatcher._build_handoff_context(
        session_id="bot:group:room",
        result=result,
        explanation=explanation,
    )

    assert isinstance(handoff, ReviewHandoffContext)
    assert handoff.review_run_id == "run_abc"
    assert handoff.explanation is explanation
    assert handoff.overflow_summaries == [
        SummaryHandoffEntry(
            content="Old overflow summary.",
            msg_log_start=1,
            msg_log_end=10,
            msg_count=10,
        )
    ]
    assert handoff.block_digests == [
        SummaryHandoffEntry(
            content="Block 0 digest.",
            block_index=0,
            msg_log_start=11,
            msg_log_end=20,
            msg_count=10,
        ),
        SummaryHandoffEntry(
            content="Block 1 digest.",
            block_index=1,
            msg_log_start=21,
            msg_log_end=30,
            msg_count=10,
        ),
    ]
    assert handoff.recent_active_chat_summary == "Previous active chat context."

    assert fake_summary_service.list_by_run_id_calls[0] == (
        "run_abc", SummaryType.OVERFLOW_COMPRESSION,
    )
    assert fake_summary_service.list_by_run_id_calls[1] == (
        "run_abc", SummaryType.BLOCK_DIGEST,
    )
    assert fake_summary_service.get_latest_calls[0] == (
        "bot:group:room", SummaryType.ACTIVE_CHAT,
    )


@pytest.mark.asyncio
async def test_dispatcher_build_handoff_context_without_summary_service() -> None:
    from shinbot.agent.coordinators.review.models import (
        ActiveChatBootstrapResult,
        ReplyDecisionResult,
        ReviewScanResult,
        ReviewWorkflowExplanation,
        ReviewWorkflowResult,
    )
    from shinbot.agent.services.summaries import ReviewHandoffContext

    explanation = ReviewWorkflowExplanation(
        review_run_id="run_abc",
        review_started_at=100.0,
    )
    result = ReviewWorkflowResult(
        review_run_id="run_abc",
        scan=ReviewScanResult(),
        reply=ReplyDecisionResult(),
        bootstrap=ActiveChatBootstrapResult(),
        review_started_at=100.0,
    )
    dispatcher = ActiveReplyDispatcher()

    handoff = await dispatcher._build_handoff_context(
        session_id="bot:group:room",
        result=result,
        explanation=explanation,
    )

    assert isinstance(handoff, ReviewHandoffContext)
    assert handoff.overflow_summaries == []
    assert handoff.block_digests == []
    assert handoff.recent_active_chat_summary is None


@pytest.mark.asyncio
async def test_dispatcher_build_handoff_context_filters_stale_active_summary() -> None:
    import time

    from shinbot.agent.coordinators.review.models import (
        ActiveChatBootstrapResult,
        ReplyDecisionResult,
        ReviewScanResult,
        ReviewWorkflowConfig,
        ReviewWorkflowExplanation,
        ReviewWorkflowResult,
    )

    explanation = ReviewWorkflowExplanation(
        review_run_id="run_abc",
        review_started_at=100.0,
    )
    result = ReviewWorkflowResult(
        review_run_id="run_abc",
        scan=ReviewScanResult(),
        reply=ReplyDecisionResult(),
        bootstrap=ActiveChatBootstrapResult(),
        review_started_at=100.0,
    )

    stale_record = _HandoffFakeSummaryRecord(
        content="Stale summary.",
        created_at=time.time() - 3600,
    )
    fake_summary_service = _HandoffFakeSummaryService(active_record=stale_record)
    dispatcher = ActiveReplyDispatcher(
        summary_service=fake_summary_service,
        review_config=ReviewWorkflowConfig(active_chat_summary_max_age_seconds=1800.0),
    )

    handoff = await dispatcher._build_handoff_context(
        session_id="bot:group:room",
        result=result,
        explanation=explanation,
    )

    assert handoff.recent_active_chat_summary is None
