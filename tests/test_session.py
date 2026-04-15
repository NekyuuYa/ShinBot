"""Tests for session state management."""

from shinbot.core.state.session import (
    Session,
    SessionConfig,
    SessionManager,
    build_session_id,
    session_from_event,
)
from shinbot.schema.events import Channel, Guild, UnifiedEvent, User


def _make_event(
    *,
    channel_id: str = "ch-1",
    channel_type: int = 0,
    user_id: str = "user-1",
    guild_id: str | None = None,
    platform: str = "test",
    channel_name: str = "",
) -> UnifiedEvent:
    guild = Guild(id=guild_id) if guild_id else None
    return UnifiedEvent(
        type="message-created",
        platform=platform,
        user=User(id=user_id),
        channel=Channel(id=channel_id, type=channel_type, name=channel_name),
        guild=guild,
    )


class TestSessionConfig:
    def test_defaults(self):
        cfg = SessionConfig()
        assert cfg.prefixes == ["/"]
        assert cfg.llm_enabled is True
        assert cfg.is_muted is False
        assert cfg.audit_enabled is False

    def test_custom(self):
        cfg = SessionConfig(prefixes=["/", "#"], is_muted=True)
        assert cfg.prefixes == ["/", "#"]
        assert cfg.is_muted is True


class TestBuildSessionId:
    def test_private_message(self):
        event = _make_event(channel_type=1, user_id="user-42")
        sid = build_session_id("bot1", event)
        assert sid == "bot1:private:user-42"

    def test_group_flat(self):
        event = _make_event(channel_id="group-100", channel_type=0)
        sid = build_session_id("bot1", event)
        assert sid == "bot1:group:group-100"

    def test_group_nested(self):
        event = _make_event(channel_id="chan-5", guild_id="guild-3", channel_type=0)
        sid = build_session_id("bot1", event)
        assert sid == "bot1:group:guild-3:chan-5"


class TestSessionFromEvent:
    def test_private(self):
        event = _make_event(channel_type=1, user_id="u1", channel_name="Alice")
        session = session_from_event("inst1", event)
        assert session.id == "inst1:private:u1"
        assert session.session_type == "private"
        assert session.is_private is True
        assert session.display_name == "Alice"

    def test_group(self):
        event = _make_event(channel_id="g100", channel_name="MyGroup")
        session = session_from_event("inst1", event)
        assert session.id == "inst1:group:g100"
        assert session.session_type == "group"
        assert session.is_group is True


class TestSession:
    def test_touch(self):
        event = _make_event()
        session = session_from_event("inst1", event)
        old = session.last_active
        session.touch()
        assert session.last_active >= old

    def test_is_muted(self):
        session = Session(
            id="x:group:1",
            instance_id="x",
            session_type="group",
            config=SessionConfig(is_muted=True),
        )
        assert session.is_muted is True

    def test_default_permission_group(self):
        session = Session(id="x:group:1", instance_id="x", session_type="group")
        assert session.permission_group == "default"


class TestSessionManager:
    def test_get_or_create_creates(self):
        mgr = SessionManager()
        event = _make_event(channel_id="g-1")
        session = mgr.get_or_create("inst1", event)
        assert session.id == "inst1:group:g-1"
        assert len(mgr) == 1

    def test_get_or_create_returns_existing(self):
        mgr = SessionManager()
        event = _make_event(channel_id="g-1")
        s1 = mgr.get_or_create("inst1", event)
        s2 = mgr.get_or_create("inst1", event)
        assert s1 is s2
        assert len(mgr) == 1

    def test_get_nonexistent(self):
        mgr = SessionManager()
        assert mgr.get("nonexistent") is None

    def test_remove(self):
        mgr = SessionManager()
        event = _make_event(channel_id="g-1")
        mgr.get_or_create("inst1", event)
        removed = mgr.remove("inst1:group:g-1")
        assert removed is not None
        assert len(mgr) == 0

    def test_all_sessions(self):
        mgr = SessionManager()
        mgr.get_or_create("inst1", _make_event(channel_id="g-1"))
        mgr.get_or_create("inst1", _make_event(channel_id="g-2"))
        assert len(mgr.all_sessions) == 2

    def test_sessions_for_instance(self):
        mgr = SessionManager()
        mgr.get_or_create("inst1", _make_event(channel_id="g-1"))
        mgr.get_or_create("inst2", _make_event(channel_id="g-2"))
        assert len(mgr.sessions_for_instance("inst1")) == 1
        assert len(mgr.sessions_for_instance("inst2")) == 1
