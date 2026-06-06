"""Tests for security permission engine."""

import pytest

from shinbot.core.security.permission import (
    PermissionEngine,
    PermissionGroup,
    check_permission,
    merge_permissions,
)


class TestCheckPermission:
    def test_exact_match(self):
        assert check_permission("cmd.help", {"cmd.help"}) is True

    def test_no_match(self):
        assert check_permission("cmd.admin", {"cmd.help"}) is False

    def test_wildcard_all(self):
        assert check_permission("anything.here", {"*"}) is True

    def test_wildcard_subtree(self):
        assert check_permission("tools.weather", {"tools.*"}) is True
        assert check_permission("tools.weather.admin", {"tools.*"}) is True

    def test_wildcard_does_not_cross_tree(self):
        assert check_permission("system.reboot", {"tools.*"}) is False

    def test_explicit_deny(self):
        assert check_permission("tools.weather", {"tools.*", "-tools.weather"}) is False

    def test_deny_takes_priority(self):
        assert check_permission("cmd.help", {"*", "-cmd.help"}) is False

    def test_deny_wildcard(self):
        assert check_permission("tools.weather", {"-tools.*"}) is False

    def test_empty_set(self):
        assert check_permission("anything", set()) is False

    def test_root_deny(self):
        assert check_permission("anything", {"-*"}) is False


class TestMergePermissions:
    def test_merge_sets(self):
        result = merge_permissions({"a", "b"}, {"c"})
        assert result == {"a", "b", "c"}

    def test_merge_groups(self):
        g1 = PermissionGroup(id="a", permissions={"x"})
        g2 = PermissionGroup(id="b", permissions={"y"})
        result = merge_permissions(g1, g2)
        assert result == {"x", "y"}

    def test_merge_mixed(self):
        g = PermissionGroup(id="a", permissions={"x"})
        result = merge_permissions(g, {"y", "z"})
        assert result == {"x", "y", "z"}


class TestPermissionGroup:
    def test_grant(self):
        g = PermissionGroup(id="test")
        g.grant("cmd.help")
        assert "cmd.help" in g.permissions

    def test_revoke(self):
        g = PermissionGroup(id="test", permissions={"cmd.help", "cmd.ping"})
        g.revoke("cmd.help")
        assert "cmd.help" not in g.permissions

    def test_deny(self):
        g = PermissionGroup(id="test")
        g.deny("tools.dangerous")
        assert "-tools.dangerous" in g.permissions


class TestPermissionEngine:
    def _make_engine(self) -> PermissionEngine:
        return PermissionEngine()

    def test_builtin_groups_exist(self):
        engine = self._make_engine()
        assert engine.get_group("default") is not None
        assert engine.get_group("admin") is not None
        assert engine.get_group("owner") is not None

    def test_default_group_perms(self):
        engine = self._make_engine()
        g = engine.get_group("default")
        assert "cmd.help" in g.permissions
        assert "cmd.ping" in g.permissions
        assert "cmd.mute" not in g.permissions

    def test_resolve_base_only(self):
        engine = self._make_engine()
        perms = engine.resolve("inst1", "inst1:group:g1", "user1")
        # Should only have default group perms
        assert "cmd.help" in perms
        assert "tools.weather" not in perms  # not admin

    def test_resolve_with_global_binding(self):
        engine = self._make_engine()
        engine.bind("inst1:user1", "owner")
        perms = engine.resolve("inst1", "inst1:group:g1", "user1")
        assert "*" in perms  # from owner group
        assert "cmd.help" in perms  # from default base

    def test_resolve_merges_multiple_global_binding_groups(self):
        engine = self._make_engine()
        engine.add_group(PermissionGroup(id="moderator", permissions={"cmd.mute"}))
        engine.add_group(PermissionGroup(id="search_user", permissions={"tools.search.query"}))

        engine.bind_group("inst1:user1", "moderator")
        engine.bind_group("inst1:user1", "search_user")

        perms = engine.resolve("inst1", "inst1:group:g1", "user1")
        assert "cmd.mute" in perms
        assert "tools.search.query" in perms
        assert "cmd.help" in perms

    def test_resolve_merges_multiple_session_binding_groups(self):
        engine = self._make_engine()
        engine.add_group(PermissionGroup(id="moderator", permissions={"cmd.mute"}))
        engine.add_group(PermissionGroup(id="search_user", permissions={"tools.search.query"}))

        engine.bind_group("inst1:group:g1.user1", "moderator")
        engine.bind_group("inst1:group:g1.user1", "search_user")

        perms = engine.resolve("inst1", "inst1:group:g1", "user1")
        assert "cmd.mute" in perms
        assert "tools.search.query" in perms

    def test_resolve_with_session_binding(self):
        engine = self._make_engine()
        engine.bind("inst1:group:g1.user1", "owner")
        perms = engine.resolve("inst1", "inst1:group:g1", "user1")
        assert "*" in perms

    def test_resolve_owner_gets_wildcard(self):
        engine = self._make_engine()
        engine.bind("inst1:owner1", "owner")
        perms = engine.resolve("inst1", "inst1:group:g1", "owner1")
        assert "*" in perms

    def test_check_permission(self):
        engine = self._make_engine()
        engine.bind("inst1:user1", "owner")
        assert engine.check("tools.weather", "inst1", "inst1:group:g1", "user1") is True
        assert engine.check("sys.reboot", "inst1", "inst1:group:g1", "user1") is True

    def test_runtime_admin_group_grants_permissions_without_binding(self):
        engine = self._make_engine()

        perms = engine.resolve(
            "inst1",
            "inst1:group:g1",
            "user1",
            runtime_group_ids=("admin",),
        )

        assert "tools.*" in perms
        assert engine.groups_for_key("inst1:user1") == ()
        assert (
            engine.check(
                "cmd.mute",
                "inst1",
                "inst1:group:g1",
                "user1",
                runtime_group_ids=("admin",),
            )
            is True
        )

    def test_runtime_managed_admin_group_cannot_be_bound(self):
        engine = self._make_engine()
        engine.add_group(PermissionGroup(id="moderator", permissions={"cmd.mute"}))

        with pytest.raises(ValueError, match="runtime-managed"):
            engine.bind("inst1:user1", "admin")
        with pytest.raises(ValueError, match="runtime-managed"):
            engine.bind_group("inst1:user1", "admin")
        with pytest.raises(ValueError, match="runtime-managed"):
            engine.set_groups_for_key("inst1:user1", ["admin", "moderator"])

        assert engine.groups_for_key("inst1:user1") == ()

    def test_deprecated_bot_admin_binding_prefix_is_ignored(self):
        engine = self._make_engine()
        engine.bind("__bot_admin__:bot-main:platform:user-admin", "owner")
        assert (
            engine.check(
                "cmd.mute",
                "bot-main",
                "bot-main:group:g1",
                "platform:user-admin",
            )
            is False
        )

    def test_deprecated_bot_admin_binding_prefix_does_not_merge_groups(self):
        engine = self._make_engine()
        engine.add_group(PermissionGroup(id="moderator", permissions={"cmd.mute"}))

        engine.bind_group("__bot_admin__:bot-main:platform:user-admin", "owner")
        engine.bind_group("__bot_admin__:bot-main:platform:user-admin", "moderator")

        perms = engine.resolve(
            "bot-main",
            "bot-main:group:g1",
            "platform:user-admin",
        )
        assert "*" not in perms
        assert "cmd.mute" not in perms

    def test_check_mute_denied_for_default_user(self):
        engine = self._make_engine()
        assert engine.check("cmd.mute", "bot-main", "bot-main:group:g1", "user1") is False

    def test_check_denied_for_default_user(self):
        engine = self._make_engine()
        assert engine.check("tools.weather", "inst1", "inst1:group:g1", "nobody") is False

    def test_unbind(self):
        engine = self._make_engine()
        engine.bind("inst1:user1", "owner")
        engine.unbind("inst1:user1")
        perms = engine.resolve("inst1", "inst1:group:g1", "user1")
        assert "*" not in perms

    def test_bind_replaces_existing_groups_and_get_binding_returns_stable_first(self):
        engine = self._make_engine()
        engine.add_group(PermissionGroup(id="moderator", permissions={"cmd.mute"}))
        engine.add_group(PermissionGroup(id="search_user", permissions={"tools.search.query"}))

        engine.bind_group("inst1:user1", "moderator")
        engine.bind_group("inst1:user1", "search_user")

        assert engine.groups_for_key("inst1:user1") == ("moderator", "search_user")
        assert engine.get_binding("inst1:user1") == "moderator"

        engine.bind("inst1:user1", "owner")

        assert engine.groups_for_key("inst1:user1") == ("owner",)
        assert engine.get_binding("inst1:user1") == "owner"

    def test_unbind_group_removes_one_group_and_prunes_empty_binding(self):
        engine = self._make_engine()
        engine.add_group(PermissionGroup(id="moderator", permissions={"cmd.mute"}))

        engine.bind_group("inst1:user1", "owner", source="test")
        engine.bind_group("inst1:user1", "moderator", source="test")

        engine.unbind_group("inst1:user1", "owner", source="test")

        assert engine.groups_for_key("inst1:user1") == ("moderator",)
        assert "inst1:user1" in engine.binding_keys()

        engine.unbind_group("inst1:user1", "moderator", source="test")

        assert engine.groups_for_key("inst1:user1") == ()
        assert "inst1:user1" not in engine.binding_keys()

    def test_set_groups_for_key_replaces_groups(self):
        engine = self._make_engine()
        engine.add_group(PermissionGroup(id="moderator", permissions={"cmd.mute"}))

        engine.set_groups_for_key("inst1:user1", ["owner", "moderator"])
        assert engine.groups_for_key("inst1:user1") == ("moderator", "owner")

        engine.set_groups_for_key("inst1:user1", ["owner"])
        assert engine.groups_for_key("inst1:user1") == ("owner",)

        engine.set_groups_for_key("inst1:user1", [])
        assert engine.groups_for_key("inst1:user1") == ()
        assert "inst1:user1" not in engine.binding_keys()

    def test_set_groups_for_key_unknown_group_keeps_existing_binding(self):
        engine = self._make_engine()
        engine.bind("inst1:user1", "owner")

        with pytest.raises(ValueError, match="Unknown permission group"):
            engine.set_groups_for_key("inst1:user1", ["owner", "missing"])

        assert engine.groups_for_key("inst1:user1") == ("owner",)

    def test_bind_group_unknown_group_does_not_create_binding(self):
        engine = self._make_engine()

        with pytest.raises(ValueError, match="Unknown permission group"):
            engine.bind_group("inst1:user1", "missing")

        assert engine.groups_for_key("inst1:user1") == ()
        assert "inst1:user1" not in engine.binding_keys()

    def test_resolve_custom_session_base_group(self):
        engine = self._make_engine()
        engine.add_group(PermissionGroup(id="session_mod", permissions={"cmd.mute"}))

        perms = engine.resolve(
            "inst1",
            "inst1:group:g1",
            "user1",
            session_base_group="session_mod",
        )

        assert "cmd.mute" in perms
        assert "cmd.help" not in perms

    def test_resolve_ignores_removed_bound_group(self):
        engine = self._make_engine()
        engine.add_group(PermissionGroup(id="moderator", permissions={"cmd.mute"}))
        engine.bind_group("inst1:user1", "owner")
        engine.bind_group("inst1:user1", "moderator")

        engine.remove_group("moderator")

        perms = engine.resolve("inst1", "inst1:group:g1", "user1")
        assert "*" in perms
        assert "cmd.mute" not in perms

    def test_bind_unknown_group_raises(self):
        engine = self._make_engine()
        with pytest.raises(ValueError, match="Unknown permission group"):
            engine.bind("inst1:user1", "nonexistent")

    def test_add_custom_group(self):
        engine = self._make_engine()
        custom = PermissionGroup(id="mod", permissions={"cmd.*", "tools.weather"})
        engine.add_group(custom)
        engine.bind("inst1:user1", "mod")
        assert engine.check("cmd.help", "inst1", "inst1:group:g1", "user1") is True
        assert engine.check("tools.weather", "inst1", "inst1:group:g1", "user1") is True
        assert engine.check("sys.reboot", "inst1", "inst1:group:g1", "user1") is False
