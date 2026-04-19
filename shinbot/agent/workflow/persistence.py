"""Persistence helpers for workflow runtime artifacts."""

from __future__ import annotations

from typing import TYPE_CHECKING

from shinbot.persistence.records import PromptSnapshotRecord

if TYPE_CHECKING:
    from shinbot.agent.attention.models import WorkflowRunRecord
    from shinbot.agent.prompt_manager import PromptSnapshot
    from shinbot.persistence.engine import DatabaseManager


def persist_prompt_snapshot(
    database: DatabaseManager,
    snapshot: PromptSnapshot,
) -> None:
    """Persist one assembled prompt snapshot."""

    database.prompt_snapshots.insert(
        PromptSnapshotRecord(
            id=snapshot.id,
            profile_id=snapshot.profile_id,
            caller=snapshot.caller,
            session_id=snapshot.session_id,
            instance_id=snapshot.instance_id,
            route_id=snapshot.route_id,
            model_id=snapshot.model_id,
            prompt_signature=snapshot.prompt_signature,
            cache_key=snapshot.cache_key,
            messages=snapshot.full_messages,
            tools=snapshot.full_tools,
            compatibility_used=snapshot.compatibility_used,
            created_at=snapshot.timestamp,
        )
    )


def persist_workflow_run(
    database: DatabaseManager,
    record: WorkflowRunRecord,
) -> None:
    """Persist one workflow run audit record."""

    database.workflow_runs.insert(record)
