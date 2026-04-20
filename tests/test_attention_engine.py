"""Unit tests for the attention engine core logic."""

from __future__ import annotations

import math
import time

import pytest

from shinbot.agent.attention.engine import AttentionConfig, AttentionEngine, _clamp, weight_curve
from shinbot.agent.attention.models import SenderWeightState, SessionAttentionState
from shinbot.persistence.config import DatabaseConfig
from shinbot.persistence.engine import DatabaseManager


@pytest.fixture()
def db(tmp_path):
    """Create a temporary DatabaseManager for testing."""
    db_path = tmp_path / "test.db"
    url = f"sqlite:///{db_path}"
    dm = DatabaseManager(DatabaseConfig(url=url, sqlite_path=db_path))
    dm.initialize()
    return dm


@pytest.fixture()
def engine(db):
    return AttentionEngine(AttentionConfig(), db.attention)


@pytest.fixture()
def config():
    return AttentionConfig()


# ── weight_curve tests ──────────────────────────────────────────────


class TestWeightCurve:
    def test_neutral_maps_to_one(self):
        assert weight_curve(0.0) == 1.0

    def test_positive_amplifies(self):
        assert weight_curve(1.0) == 2.0
        assert weight_curve(2.0) == 4.0

    def test_negative_attenuates(self):
        assert weight_curve(-1.0) == 0.5
        assert weight_curve(-2.0) == 0.25

    def test_symmetry(self):
        for score in [0.5, 1.0, 1.5]:
            assert abs(weight_curve(score) * weight_curve(-score) - 1.0) < 1e-10


# ── clamp tests ─────────────────────────────────────────────────────


class TestClamp:
    def test_within_range(self):
        value, status = _clamp(1.5, 0.0, 3.0)
        assert value == 1.5
        assert status == "applied"

    def test_clamp_to_max(self):
        value, status = _clamp(5.0, 0.0, 3.0)
        assert value == 3.0
        assert status == "clamped_to_max"

    def test_clamp_to_min(self):
        value, status = _clamp(-1.0, 0.0, 3.0)
        assert value == 0.0
        assert status == "clamped_to_min"


# ── Time decay tests ───────────────────────────────────────────────


class TestTimeDecay:
    def test_no_decay_at_zero_dt(self, engine):
        state = SessionAttentionState(session_id="test", attention_value=10.0)
        now = state.last_update_at
        result = engine.apply_time_decay(state, now)
        assert result.attention_value == 10.0

    def test_no_decay_within_idle_grace(self, engine):
        now = time.time()
        state = SessionAttentionState(
            session_id="test",
            attention_value=10.0,
            runtime_threshold_offset=3.0,
            last_update_at=now - 40,
        )
        result = engine.apply_time_decay(state, now)
        assert result.attention_value == 10.0
        assert result.runtime_threshold_offset == 3.0

    def test_exponential_decay_after_idle_grace(self, engine):
        now = time.time()
        state = SessionAttentionState(
            session_id="test",
            attention_value=10.0,
            last_update_at=now - 220,
        )
        result = engine.apply_time_decay(state, now)
        effective_dt = 220 - engine.config.decay_idle_grace_seconds
        expected = 10.0 * math.exp(-engine.config.decay_k * effective_dt)
        assert abs(result.attention_value - expected) < 1e-10

    def test_runtime_offset_regression(self, engine):
        now = time.time()
        state = SessionAttentionState(
            session_id="test",
            runtime_threshold_offset=3.0,
            last_update_at=now - 220,
        )
        result = engine.apply_time_decay(state, now)
        effective_dt = 220 - engine.config.decay_idle_grace_seconds
        expected_offset = 3.0 * math.exp(-engine.config.runtime_threshold_decay_k * effective_dt)
        assert abs(result.runtime_threshold_offset - expected_offset) < 1e-10

    def test_sender_weight_decay(self, engine):
        now = time.time()
        sw = SenderWeightState(
            session_id="test",
            sender_id="user1",
            runtime_weight=2.0,
            last_runtime_adjust_at=now - 30,
        )
        result = engine.apply_sender_weight_decay(sw, now)
        expected = 2.0 * math.exp(-engine.config.runtime_weight_decay_k * 30)
        assert abs(result.runtime_weight - expected) < 1e-10


# ── Contribution tests ──────────────────────────────────────────────


class TestContribution:
    def test_base_contribution(self, engine):
        c = engine.compute_contribution(
            sender_factor=1.0,
            is_mentioned=False,
            is_reply_to_bot=False,
            recent_mention_count=0,
        )
        assert c == engine.config.base_gain

    def test_mention_bonus(self, engine):
        c = engine.compute_contribution(
            sender_factor=1.0,
            is_mentioned=True,
            is_reply_to_bot=False,
            recent_mention_count=0,
        )
        assert c == engine.config.base_gain + engine.config.mention_bonus

    def test_reply_bonus(self, engine):
        c = engine.compute_contribution(
            sender_factor=1.0,
            is_mentioned=False,
            is_reply_to_bot=True,
            recent_mention_count=0,
        )
        assert c == engine.config.base_gain + engine.config.reply_bonus

    def test_burst_amplification(self, engine):
        # With 3 recent mentions, feature_bonus should be amplified
        c_single = engine.compute_contribution(
            sender_factor=1.0,
            is_mentioned=True,
            is_reply_to_bot=False,
            recent_mention_count=1,  # only 1, no burst
        )
        c_burst = engine.compute_contribution(
            sender_factor=1.0,
            is_mentioned=True,
            is_reply_to_bot=False,
            recent_mention_count=3,  # burst!
        )
        # Burst should produce larger contribution
        assert c_burst > c_single

    def test_sender_factor_scales(self, engine):
        c_low = engine.compute_contribution(
            sender_factor=0.25,  # very low weight
            is_mentioned=False,
            is_reply_to_bot=False,
            recent_mention_count=0,
        )
        c_high = engine.compute_contribution(
            sender_factor=4.0,  # very high weight
            is_mentioned=False,
            is_reply_to_bot=False,
            recent_mention_count=0,
        )
        assert c_high > c_low
        assert c_low == engine.config.base_gain * 0.25
        assert c_high == engine.config.base_gain * 4.0

    def test_attention_multiplier_scales_whole_contribution(self, engine):
        c = engine.compute_contribution(
            sender_factor=1.0,
            is_mentioned=True,
            is_reply_to_bot=False,
            recent_mention_count=0,
            attention_multiplier=0.2,
        )
        assert c == (engine.config.base_gain + engine.config.mention_bonus) * 0.2

    def test_unanswered_mention_streak_doubles_contribution(self, engine):
        c1 = engine.compute_contribution(
            sender_factor=1.0,
            is_mentioned=True,
            is_reply_to_bot=False,
            recent_mention_count=0,
            mention_streak=1,
        )
        c2 = engine.compute_contribution(
            sender_factor=1.0,
            is_mentioned=True,
            is_reply_to_bot=False,
            recent_mention_count=0,
            mention_streak=2,
        )
        assert c2 == pytest.approx(c1 * 2.0)


# ── Effective threshold tests ───────────────────────────────────────


class TestEffectiveThreshold:
    def test_base_threshold(self, engine):
        state = SessionAttentionState(session_id="test")
        assert engine.effective_threshold(state) == engine.config.base_threshold

    def test_with_positive_offset(self, engine):
        state = SessionAttentionState(
            session_id="test",
            base_threshold=5.0,
            runtime_threshold_offset=2.0,
        )
        assert engine.effective_threshold(state) == 7.0

    def test_clamped_to_max(self, engine):
        state = SessionAttentionState(
            session_id="test",
            base_threshold=5.0,
            runtime_threshold_offset=100.0,
        )
        assert engine.effective_threshold(state) == engine.config.threshold_max

    def test_clamped_to_min(self, engine):
        state = SessionAttentionState(
            session_id="test",
            base_threshold=5.0,
            runtime_threshold_offset=-100.0,
        )
        assert engine.effective_threshold(state) == engine.config.threshold_min

    def test_fixed_base_threshold_metadata_overrides_state_value(self, engine):
        state = SessionAttentionState(
            session_id="test",
            base_threshold=5.0,
            runtime_threshold_offset=2.0,
            metadata={engine.FIXED_BASE_THRESHOLD_METADATA_KEY: 15.0},
        )
        assert engine.effective_threshold(state) == 17.0


# ── Integration: update_attention ───────────────────────────────────


class TestUpdateAttention:
    def test_accumulates_attention(self, engine, db):
        # Pre-create session in DB (sessions table FK)
        with db.connect() as conn:
            conn.execute(
                """
                INSERT INTO sessions (id, instance_id, session_type, created_at, last_active)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("s1", "inst1", "group", time.time(), time.time()),
            )

        state1, triggered1 = engine.update_attention(
            "s1",
            sender_id="u1",
            msg_log_id=1,
        )
        assert state1.attention_value > 0
        assert not triggered1  # base_gain=1.0 < threshold=5.0

    def test_triggers_at_threshold(self, engine, db):
        with db.connect() as conn:
            conn.execute(
                """
                INSERT INTO sessions (id, instance_id, session_type, created_at, last_active)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("s2", "inst1", "group", time.time(), time.time()),
            )

        # Use a very low threshold for testing
        engine.config.base_threshold = 2.0

        now = time.time()
        triggered = False
        for i in range(10):
            state, t = engine.update_attention(
                "s2",
                sender_id="u1",
                msg_log_id=i + 1,
                now=now,
            )
            if t:
                triggered = True
                break

        assert triggered

    def test_mention_helps_trigger(self, engine, db):
        with db.connect() as conn:
            conn.execute(
                """
                INSERT INTO sessions (id, instance_id, session_type, created_at, last_active)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("s3", "inst1", "group", time.time(), time.time()),
            )

        engine.config.base_threshold = 3.0
        now = time.time()

        # Single mention should contribute more
        state, triggered = engine.update_attention(
            "s3",
            sender_id="u1",
            msg_log_id=1,
            is_mentioned=True,
            now=now,
        )
        # base_gain(1.0) * factor(1.0) + mention_bonus(1.5) = 2.5
        assert abs(state.attention_value - 2.5) < 0.1

    def test_consecutive_mentions_double_until_reset(self, engine, db):
        with db.connect() as conn:
            conn.execute(
                """
                INSERT INTO sessions (id, instance_id, session_type, created_at, last_active)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("s4", "inst1", "group", time.time(), time.time()),
            )

        now = time.time()
        state1, _ = engine.update_attention(
            "s4",
            sender_id="u1",
            msg_log_id=1,
            is_mentioned=True,
            now=now,
        )
        contribution1 = state1.attention_value

        state2, _ = engine.update_attention(
            "s4",
            sender_id="u1",
            msg_log_id=2,
            is_mentioned=True,
            now=now,
        )
        contribution2 = state2.attention_value - state1.attention_value
        assert contribution2 == pytest.approx(contribution1 * 2.0)

        # Simulate that workflow produced a visible reply.
        engine.reset_unanswered_mention_streak("s4")

        state3, _ = engine.update_attention(
            "s4",
            sender_id="u1",
            msg_log_id=3,
            is_mentioned=True,
            now=now,
        )
        contribution3 = state3.attention_value - state2.attention_value
        assert contribution3 == pytest.approx(contribution1)

    def test_mention_chain_only_counts_mentions_to_bot(self, engine, db):
        with db.connect() as conn:
            conn.execute(
                """
                INSERT INTO sessions (id, instance_id, session_type, created_at, last_active)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("s5", "inst1", "group", time.time(), time.time()),
            )

        now = time.time()
        state1, _ = engine.update_attention(
            "s5",
            sender_id="u1",
            msg_log_id=1,
            is_mentioned=True,
            now=now,
        )
        contribution1 = state1.attention_value

        # Simulate "@other" in group chat: not a mention to bot.
        state2, _ = engine.update_attention(
            "s5",
            sender_id="u1",
            msg_log_id=2,
            is_mentioned=False,
            attention_multiplier=engine.config.mention_other_multiplier,
            now=now,
        )
        _ = state2.attention_value - state1.attention_value

        # Next "@bot" should be second effective bot-mention, so x2 (not x4).
        state3, _ = engine.update_attention(
            "s5",
            sender_id="u1",
            msg_log_id=3,
            is_mentioned=True,
            now=now,
        )
        contribution3 = state3.attention_value - state2.attention_value
        assert contribution3 == pytest.approx(contribution1 * 2.0)


# ── Reply fatigue tests ────────────────────────────────────────────


class TestReplyFatigue:
    def test_fatigue_increases_offset(self, engine, db):
        with db.connect() as conn:
            conn.execute(
                """
                INSERT INTO sessions (id, instance_id, session_type, created_at, last_active)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("sf1", "inst1", "group", time.time(), time.time()),
            )

        state = engine.repo.get_or_create_attention("sf1")
        original_offset = state.runtime_threshold_offset
        state = engine.apply_reply_fatigue(state)
        assert state.runtime_threshold_offset > original_offset
        assert state.is_cooling_down


# ── Consume batch tests ─────────────────────────────────────────────


class TestConsumeBatch:
    def test_deducts_threshold(self, engine):
        state = SessionAttentionState(
            session_id="test",
            attention_value=8.0,
            base_threshold=5.0,
        )
        result = engine.consume_batch(state)
        assert abs(result.attention_value - 3.0) < 1e-10

    def test_floor_at_zero(self, engine):
        state = SessionAttentionState(
            session_id="test",
            attention_value=3.0,
            base_threshold=5.0,
        )
        result = engine.consume_batch(state)
        assert result.attention_value == 0.0


# ── Weight adjustment tests ─────────────────────────────────────────


class TestWeightAdjustment:
    def test_adjust_sender_weight(self, engine, db):
        with db.connect() as conn:
            conn.execute(
                """
                INSERT INTO sessions (id, instance_id, session_type, created_at, last_active)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("sw1", "inst1", "group", time.time(), time.time()),
            )

        result = engine.adjust_sender_weight(
            "sw1",
            "user1",
            stable_delta=0.3,
            runtime_delta=0.5,
        )
        assert result["applied"]["stable"] == "applied"
        assert result["applied"]["runtime"] == "applied"

    def test_clamp_feedback(self, engine, db):
        with db.connect() as conn:
            conn.execute(
                """
                INSERT INTO sessions (id, instance_id, session_type, created_at, last_active)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("sw2", "inst1", "group", time.time(), time.time()),
            )

        result = engine.adjust_sender_weight(
            "sw2",
            "user1",
            stable_delta=10.0,  # way over max
        )
        assert result["applied"]["stable"] == "clamped_to_max"
        assert "上限" in result["hint"]


# ── Threshold adjustment tests ──────────────────────────────────────


class TestThresholdAdjustment:
    def test_adjust_threshold(self, engine, db):
        with db.connect() as conn:
            conn.execute(
                """
                INSERT INTO sessions (id, instance_id, session_type, created_at, last_active)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("th1", "inst1", "group", time.time(), time.time()),
            )

        result = engine.adjust_session_threshold("th1", offset_delta=1.5)
        assert result["applied"] == "applied"
        assert result["effective_threshold"] == 6.5  # 5.0 + 1.5

    def test_threshold_clamp(self, engine, db):
        with db.connect() as conn:
            conn.execute(
                """
                INSERT INTO sessions (id, instance_id, session_type, created_at, last_active)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("th2", "inst1", "group", time.time(), time.time()),
            )

        result = engine.adjust_session_threshold("th2", offset_delta=100.0)
        assert result["effective_threshold"] == engine.config.threshold_max

    def test_adjust_threshold_uses_fixed_base_threshold(self, engine, db):
        with db.connect() as conn:
            conn.execute(
                """
                INSERT INTO sessions (id, instance_id, session_type, created_at, last_active)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("th3", "inst1", "group", time.time(), time.time()),
            )

        state = engine.repo.get_or_create_attention("th3")
        state.metadata[engine.FIXED_BASE_THRESHOLD_METADATA_KEY] = 15.0
        engine.repo.save_attention(state)

        result = engine.adjust_session_threshold("th3", offset_delta=1.0)
        assert result["effective_threshold"] == 16.0


class TestFixedThresholdOverride:
    def test_update_attention_keeps_fixed_base_threshold(self, engine, db):
        with db.connect() as conn:
            conn.execute(
                """
                INSERT INTO sessions (id, instance_id, session_type, created_at, last_active)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("ft1", "inst1", "group", time.time(), time.time()),
            )

        state = engine.repo.get_or_create_attention("ft1")
        state.metadata[engine.FIXED_BASE_THRESHOLD_METADATA_KEY] = 15.0
        engine.repo.save_attention(state)

        updated, _ = engine.update_attention(
            "ft1",
            sender_id="u1",
            msg_log_id=1,
            base_threshold=1.0,
        )

        assert updated.base_threshold == 15.0
        assert engine.effective_threshold(updated) == 15.0

    def test_inspect_state_reports_fixed_base_threshold(self, engine, db):
        with db.connect() as conn:
            conn.execute(
                """
                INSERT INTO sessions (id, instance_id, session_type, created_at, last_active)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("ft2", "inst1", "group", time.time(), time.time()),
            )

        state = engine.repo.get_or_create_attention("ft2")
        state.metadata[engine.FIXED_BASE_THRESHOLD_METADATA_KEY] = 15.0
        engine.repo.save_attention(state)

        inspected = engine.inspect_state("ft2")
        assert inspected["base_threshold"] == 15.0
        assert inspected["fixed_base_threshold"] == 15.0


# ── Repository tests ────────────────────────────────────────────────


class TestRepository:
    def test_attention_crud(self, db):
        with db.connect() as conn:
            conn.execute(
                """
                INSERT INTO sessions (id, instance_id, session_type, created_at, last_active)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("repo1", "inst1", "group", time.time(), time.time()),
            )

        repo = db.attention
        state = repo.get_or_create_attention("repo1")
        assert state.session_id == "repo1"
        assert state.attention_value == 0.0

        state.attention_value = 3.5
        repo.save_attention(state)

        loaded = repo.get_attention("repo1")
        assert loaded is not None
        assert abs(loaded.attention_value - 3.5) < 1e-10

    def test_sender_weight_crud(self, db):
        with db.connect() as conn:
            conn.execute(
                """
                INSERT INTO sessions (id, instance_id, session_type, created_at, last_active)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("repo2", "inst1", "group", time.time(), time.time()),
            )

        repo = db.attention
        sw = repo.get_or_create_sender_weight("repo2", "user1")
        assert sw.stable_weight == 0.0

        sw.stable_weight = 0.5
        repo.save_sender_weight(sw)

        loaded = repo.get_sender_weight("repo2", "user1")
        assert loaded is not None
        assert loaded.stable_weight == 0.5

    def test_list_sender_weights(self, db):
        with db.connect() as conn:
            conn.execute(
                """
                INSERT INTO sessions (id, instance_id, session_type, created_at, last_active)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("repo3", "inst1", "group", time.time(), time.time()),
            )

        repo = db.attention
        repo.get_or_create_sender_weight("repo3", "user1")
        repo.get_or_create_sender_weight("repo3", "user2")
        weights = repo.list_sender_weights("repo3")
        assert len(weights) == 2

    def test_workflow_run_crud(self, db):
        with db.connect() as conn:
            conn.execute(
                """
                INSERT INTO sessions (id, instance_id, session_type, created_at, last_active)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("repo4", "inst1", "group", time.time(), time.time()),
            )

        from shinbot.agent.attention.models import WorkflowRunRecord

        record = WorkflowRunRecord(
            id="run-001",
            session_id="repo4",
            instance_id="inst1",
            response_profile="immediate",
            batch_size=5,
            trigger_attention=6.0,
            effective_threshold=5.0,
            replied=True,
            response_summary="hello",
            started_at=time.time(),
            finished_at=time.time(),
        )
        db.workflow_runs.insert(record)
        runs = db.workflow_runs.list_by_session("repo4")
        assert len(runs) == 1
        assert runs[0]["batch_size"] == 5
        assert runs[0]["response_profile"] == "immediate"
        assert runs[0]["replied"] is True
