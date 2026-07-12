"""Integration tests for durable Agent runtime ownership activation gates."""

from __future__ import annotations

import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.agent_ownership import (
    AgentRuntimeOwnershipConflict,
    AgentRuntimeOwnershipEventType,
    AgentRuntimeOwnershipEvidenceConflict,
    AgentRuntimeOwnershipGenerationConflict,
    AgentRuntimeOwnershipMigrationConflict,
    AgentRuntimeOwnershipMode,
    AgentRuntimeOwnershipRequired,
    AgentRuntimeOwnershipStatus,
)
from shinbot.persistence import DatabaseManager
from shinbot.persistence.repositories.agent_runtime_ownership import (
    AgentRuntimeOwnershipRepository,
)


def _database(tmp_path: Path) -> DatabaseManager:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database


def _insert_actor_aggregate(
    database: DatabaseManager,
    key: SessionKey,
    *,
    mailbox: bool = False,
) -> None:
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_aggregates (
                profile_id, session_id, created_at, updated_at
            ) VALUES (?, ?, 1.0, 1.0)
            """,
            (key.profile_id, key.session_id),
        )
        if mailbox:
            conn.execute(
                """
                INSERT INTO agent_session_mailbox (
                    event_id, profile_id, session_id, kind, occurred_at,
                    available_at, created_at
                ) VALUES ('event-1', ?, ?, 'MessageReceived', 1.0, 1.0, 1.0)
                """,
                (key.profile_id, key.session_id),
            )


def _insert_all_legacy_evidence(
    database: DatabaseManager,
    legacy_session_id: str,
) -> None:
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_scheduler_states (session_id, updated_at)
            VALUES (?, 1.0)
            """,
            (legacy_session_id,),
        )
        first = conn.execute(
            """
            INSERT INTO message_logs (session_id, role, created_at)
            VALUES (?, 'user', 1.0)
            """,
            (legacy_session_id,),
        ).lastrowid
        second = conn.execute(
            """
            INSERT INTO message_logs (session_id, role, created_at)
            VALUES (?, 'user', 2.0)
            """,
            (legacy_session_id,),
        ).lastrowid
        assert first is not None and second is not None
        conn.execute(
            """
            INSERT INTO agent_unread_messages (
                session_id, message_log_id, created_at
            ) VALUES (?, ?, 1.0)
            """,
            (legacy_session_id, first),
        )
        conn.execute(
            """
            INSERT INTO agent_unread_ranges (
                session_id, start_msg_log_id, end_msg_log_id,
                start_at, end_at, message_count
            ) VALUES (?, ?, ?, 1.0, 2.0, 2)
            """,
            (legacy_session_id, first, second),
        )


def test_first_claim_is_idempotent_and_survives_repository_restart(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    key = SessionKey("bot-a", "bot-a:group:room")
    repository = AgentRuntimeOwnershipRepository(database, clock=lambda: 10.0)

    first = repository.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor foundation enabled",
        legacy_session_id="instance-a:group:room",
        requested_by="test",
    )
    replay = repository.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="same decision replayed",
        legacy_session_id="instance-a:group:room",
        requested_by="other-worker",
    )

    assert first.created is True
    assert replay.created is False
    assert replay.ownership == first.ownership
    assert replay.ownership.generation == 1
    assert len(repository.list_events(key)) == 1

    restarted_database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    restarted_database.initialize()
    restored = restarted_database.agent_runtime_ownership.get(key)
    assert restored == first.ownership


def test_same_key_conflicting_mode_fails_closed(tmp_path: Path) -> None:
    database = _database(tmp_path)
    key = SessionKey("bot-a", "bot-a:group:room")
    repository = database.agent_runtime_ownership
    repository.claim(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        reason="legacy remains active",
        legacy_session_id="instance-a:group:room",
    )

    with pytest.raises(AgentRuntimeOwnershipConflict, match="already claimed"):
        repository.claim(
            key,
            AgentRuntimeOwnershipMode.ACTOR_V2,
            reason="conflicting rollout",
            legacy_session_id="instance-a:group:room",
        )

    restored = repository.get(key)
    assert restored is not None
    assert restored.mode is AgentRuntimeOwnershipMode.LEGACY


def test_actor_aggregate_and_mailbox_forbid_legacy_claim(tmp_path: Path) -> None:
    database = _database(tmp_path)
    key = SessionKey("bot-a", "bot-a:group:room")
    _insert_actor_aggregate(database, key, mailbox=True)

    with pytest.raises(AgentRuntimeOwnershipEvidenceConflict) as caught:
        database.agent_runtime_ownership.claim(
            key,
            AgentRuntimeOwnershipMode.LEGACY,
            reason="unsafe legacy selection",
        )

    assert caught.value.evidence == ("actor_aggregate", "actor_mailbox")
    assert database.agent_runtime_ownership.get(key) is None


def test_legacy_scheduler_and_unread_state_forbid_actor_claim(tmp_path: Path) -> None:
    database = _database(tmp_path)
    key = SessionKey("bot-a", "bot-a:group:room")
    legacy_session_id = "instance-a:group:room"
    _insert_all_legacy_evidence(database, legacy_session_id)

    with pytest.raises(AgentRuntimeOwnershipEvidenceConflict) as caught:
        database.agent_runtime_ownership.claim(
            key,
            AgentRuntimeOwnershipMode.ACTOR_V2,
            reason="unsafe actor selection",
            legacy_session_id=legacy_session_id,
        )

    assert caught.value.evidence == (
        "legacy_scheduler_state",
        "legacy_unread_messages",
        "legacy_unread_ranges",
    )


def test_actor_ownership_isolated_for_same_session_across_profiles(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    key_a = SessionKey("profile-a", "shared:group:room")
    key_b = SessionKey("profile-b", "shared:group:room")

    owner_a = repository.claim(
        key_a,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="profile a rollout",
        legacy_session_id="instance:group:room",
    ).ownership
    owner_b = repository.claim(
        key_b,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="profile b rollout",
        legacy_session_id="instance:group:room",
    ).ownership

    assert owner_a.key != owner_b.key
    assert repository.get(key_a) == owner_a
    assert repository.get(key_b) == owner_b


def test_legacy_alias_conflicts_with_actor_owned_profile(tmp_path: Path) -> None:
    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    actor_key = SessionKey("profile-a", "profile-a:group:room")
    legacy_key = SessionKey("profile-b", "profile-b:group:room")
    alias = "instance:group:room"
    repository.claim(
        actor_key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor profile active",
        legacy_session_id=alias,
    )

    with pytest.raises(AgentRuntimeOwnershipEvidenceConflict) as caught:
        repository.claim(
            legacy_key,
            AgentRuntimeOwnershipMode.LEGACY,
            reason="legacy would share unscoped state",
            legacy_session_id=alias,
        )

    assert caught.value.evidence == (
        "actor_v2_ownership:profile-a:profile-a:group:room",
    )


def test_only_one_legacy_owner_may_use_an_unscoped_session_alias(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    alias = "instance:group:room"
    repository.claim(
        SessionKey("profile-a", "profile-a:group:room"),
        AgentRuntimeOwnershipMode.LEGACY,
        reason="first legacy owner",
        legacy_session_id=alias,
    )

    with pytest.raises(AgentRuntimeOwnershipEvidenceConflict) as caught:
        repository.claim(
            SessionKey("profile-b", "profile-b:group:room"),
            AgentRuntimeOwnershipMode.LEGACY,
            reason="duplicate legacy owner",
            legacy_session_id=alias,
        )

    assert caught.value.evidence == (
        "legacy_ownership:profile-a:profile-a:group:room",
    )


def test_concurrent_same_mode_first_claim_creates_one_generation(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    repository = AgentRuntimeOwnershipRepository(database, clock=lambda: 20.0)
    key = SessionKey("profile-a", "profile-a:group:room")
    workers = 8
    barrier = threading.Barrier(workers)

    def claim() -> bool:
        barrier.wait()
        return repository.claim(
            key,
            AgentRuntimeOwnershipMode.ACTOR_V2,
            reason="concurrent rollout",
        ).created

    with ThreadPoolExecutor(max_workers=workers) as executor:
        created = list(executor.map(lambda _index: claim(), range(workers)))

    assert created.count(True) == 1
    assert created.count(False) == workers - 1
    restored = repository.get(key)
    assert restored is not None
    assert restored.generation == 1
    assert len(repository.list_events(key)) == 1


def test_concurrent_conflicting_first_claims_choose_exactly_one_mode(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    key = SessionKey("profile-a", "profile-a:group:room")
    barrier = threading.Barrier(2)

    def claim(mode: AgentRuntimeOwnershipMode) -> str:
        barrier.wait()
        try:
            result = repository.claim(key, mode, reason=f"claim {mode.value}")
        except AgentRuntimeOwnershipConflict:
            return "conflict"
        return result.ownership.mode.value

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(
            executor.map(
                claim,
                (
                    AgentRuntimeOwnershipMode.LEGACY,
                    AgentRuntimeOwnershipMode.ACTOR_V2,
                ),
            )
        )

    assert outcomes.count("conflict") == 1
    selected = database.agent_runtime_ownership.get(key)
    assert selected is not None
    assert outcomes.count(selected.mode.value) == 1
    assert len(repository.list_events(key)) == 1


def test_migration_uses_generation_cas_and_target_evidence_cleanup(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    repository = AgentRuntimeOwnershipRepository(database, clock=lambda: 30.0)
    key = SessionKey("profile-a", "profile-a:group:room")
    legacy_session_id = "instance:group:room"
    claimed = repository.claim(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        reason="legacy baseline",
        legacy_session_id=legacy_session_id,
    ).ownership
    migrating = repository.begin_migration(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        expected_generation=claimed.generation,
        reason="begin actor migration",
        requested_by="operator",
    )

    assert migrating.status is AgentRuntimeOwnershipStatus.MIGRATING
    assert migrating.mode is AgentRuntimeOwnershipMode.LEGACY
    assert migrating.pending_mode is AgentRuntimeOwnershipMode.ACTOR_V2
    assert migrating.generation == 2
    with pytest.raises(AgentRuntimeOwnershipGenerationConflict):
        repository.complete_migration(
            key,
            expected_generation=1,
            reason="stale completion",
        )
    with pytest.raises(AgentRuntimeOwnershipMigrationConflict):
        repository.claim(
            key,
            AgentRuntimeOwnershipMode.LEGACY,
            reason="claim during migration",
            legacy_session_id=legacy_session_id,
        )

    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_scheduler_states (session_id, updated_at)
            VALUES (?, 1.0)
            """,
            (legacy_session_id,),
        )
    with pytest.raises(AgentRuntimeOwnershipEvidenceConflict):
        repository.complete_migration(
            key,
            expected_generation=2,
            reason="legacy state not migrated",
        )
    with database.connect() as conn:
        conn.execute(
            "DELETE FROM agent_scheduler_states WHERE session_id = ?",
            (legacy_session_id,),
        )

    completed = repository.complete_migration(
        key,
        expected_generation=2,
        reason="legacy state migrated and verified",
        requested_by="operator",
    )

    assert completed.mode is AgentRuntimeOwnershipMode.ACTOR_V2
    assert completed.status is AgentRuntimeOwnershipStatus.ACTIVE
    assert completed.pending_mode is None
    assert completed.generation == 3
    events = repository.list_events(key)
    assert [event.event_type for event in events] == [
        AgentRuntimeOwnershipEventType.CLAIMED,
        AgentRuntimeOwnershipEventType.MIGRATION_STARTED,
        AgentRuntimeOwnershipEventType.MIGRATION_COMPLETED,
    ]
    assert [event.reason for event in events] == [
        "legacy baseline",
        "begin actor migration",
        "legacy state migrated and verified",
    ]
    assert events[1].to_mode is AgentRuntimeOwnershipMode.ACTOR_V2


def test_actor_to_legacy_migration_requires_actor_state_cleanup(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    key = SessionKey("profile-a", "profile-a:group:room")
    claimed = repository.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor baseline",
    ).ownership
    _insert_actor_aggregate(database, key)
    migrating = repository.begin_migration(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        expected_generation=claimed.generation,
        reason="begin legacy rollback",
    )

    with pytest.raises(AgentRuntimeOwnershipEvidenceConflict) as caught:
        repository.complete_migration(
            key,
            expected_generation=migrating.generation,
            reason="actor state still present",
        )
    assert "actor_aggregate" in caught.value.evidence
    with database.connect() as conn:
        conn.execute(
            """
            DELETE FROM agent_session_aggregates
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        )
    completed = repository.complete_migration(
        key,
        expected_generation=migrating.generation,
        reason="actor state removed",
    )
    assert completed.mode is AgentRuntimeOwnershipMode.LEGACY


def test_abort_migration_is_generation_checked_and_audited(tmp_path: Path) -> None:
    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    key = SessionKey("profile-a", "profile-a:group:room")
    claimed = repository.claim(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        reason="legacy baseline",
    ).ownership
    migrating = repository.begin_migration(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        expected_generation=claimed.generation,
        reason="migration experiment",
    )

    with pytest.raises(AgentRuntimeOwnershipGenerationConflict):
        repository.abort_migration(
            key,
            expected_generation=claimed.generation,
            reason="stale abort",
        )
    aborted = repository.abort_migration(
        key,
        expected_generation=migrating.generation,
        reason="operator cancelled migration",
    )

    assert aborted.mode is AgentRuntimeOwnershipMode.LEGACY
    assert aborted.status is AgentRuntimeOwnershipStatus.ACTIVE
    assert aborted.pending_mode is None
    assert aborted.generation == 3
    assert repository.list_events(key)[-1].event_type is (
        AgentRuntimeOwnershipEventType.MIGRATION_ABORTED
    )
    assert repository.list_events(key)[-1].reason == "operator cancelled migration"


def test_transactional_actor_validation_fences_mode_and_generation(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    repository = database.agent_runtime_ownership
    actor_key = SessionKey("profile-a", "profile-a:group:room")
    legacy_key = SessionKey("profile-b", "profile-b:group:other")
    actor = repository.claim(
        actor_key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor relay enabled",
    ).ownership
    repository.claim(
        legacy_key,
        AgentRuntimeOwnershipMode.LEGACY,
        reason="legacy relay disabled",
    )

    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        verified = repository.require_actor_v2_in_transaction(
            conn,
            actor_key,
            expected_generation=actor.generation,
        )
        assert verified == actor
        with pytest.raises(AgentRuntimeOwnershipGenerationConflict):
            repository.require_actor_v2_in_transaction(
                conn,
                actor_key,
                expected_generation=actor.generation + 1,
            )
        with pytest.raises(AgentRuntimeOwnershipRequired):
            repository.require_actor_v2_in_transaction(conn, legacy_key)


def test_core_and_persistence_ownership_imports_do_not_load_agent_package() -> None:
    check = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "import shinbot.core.dispatch.agent_ownership; "
                "import shinbot.persistence.repositories.agent_runtime_ownership; "
                "assert not any(name == 'shinbot.agent' or "
                "name.startswith('shinbot.agent.') for name in sys.modules)"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert check.returncode == 0, check.stderr
