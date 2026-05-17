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


__all__ = [
    "ActiveChatBootstrapApplyDecision",
    "ActiveChatBootstrapStageOutput",
    "ActiveChatCoordinator",
    "ActiveChatDisposition",
    "ActiveChatState",
    "ActiveReplyDispatcher",
    "AgentEntrySignal",
    "AgentScheduler",
    "AgentState",
    "Any",
    "DatabaseManager",
    "DatabaseReviewMessageStore",
    "DatabaseReviewSummaryStore",
    "FailingBlockDigestRunner",
    "FakeContextManager",
    "FakeModelRuntime",
    "FakeReviewScheduler",
    "FakeReviewToolManager",
    "FakeSummaryService",
    "FixedCandidateScanRunner",
    "FixedReviewPolicy",
    "LLMActiveChatBootstrapStageRunner",
    "LLMOverflowCompressionStageRunner",
    "LLMReplyDecisionStageRunner",
    "LLMReviewBlockDigestStageRunner",
    "LLMReviewScanStageRunner",
    "MessageLogRecord",
    "OverflowCompressionStageOutput",
    "PromptComponent",
    "PromptComponentKind",
    "PromptRegistry",
    "PromptStage",
    "RecordingActiveChatBootstrapRunner",
    "RecordingBlockDigestRunner",
    "RecordingOverflowCompressionRunner",
    "RecordingReplyDecisionRunner",
    "RecordingReviewContextBuilder",
    "ReplyDecisionStageOutput",
    "ReviewBlockDigestStageOutput",
    "ReviewCompletionDecision",
    "ReviewContextBuilderAdapter",
    "ReviewCoordinator",
    "ReviewLLMRunnerConfig",
    "ReviewPlan",
    "ReviewRunnerFactory",
    "ReviewRuntimeConfig",
    "ReviewScanStageOutput",
    "ReviewStageInput",
    "ReviewStageRuntimeConfig",
    "ReviewWorkflowConfig",
    "SelectingReviewScanRunner",
    "SlowBlockDigestRunner",
    "StageToolConfig",
    "UnreadMessage",
    "UnreadRange",
    "YieldingReviewScanRunner",
    "_insert_message",
    "_make_prompt_registry",
    "_strip_run_id_from_calls",
    "annotations",
    "asyncio",
    "build_review_workflow_explanation",
    "parse_json_object",
    "pytest",
    "register_active_chat_prompt_components",
    "register_review_prompt_components",
]
