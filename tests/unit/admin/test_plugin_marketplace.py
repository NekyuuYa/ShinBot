"""Unit tests for plugin marketplace registration ownership."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from shinbot.admin.plugin_marketplace import PluginMarketplaceService


def test_installer_registration_rejects_cross_owner_override(tmp_path) -> None:
    service = PluginMarketplaceService(
        bot=SimpleNamespace(),
        boot=SimpleNamespace(data_dir=tmp_path),
    )

    async def install_plugin(*_args, **_kwargs) -> None:
        return None

    service.register_installer(
        "custom",
        owner_plugin_id="plugin-a",
        install_fn=install_plugin,
    )

    with pytest.raises(ValueError, match="already owned by 'plugin-a'"):
        service.register_installer(
            "custom",
            owner_plugin_id="plugin-b",
            install_fn=install_plugin,
        )

    assert service.get_installer("custom")["owner"] == "plugin-a"


def test_installer_registration_allows_same_owner_update(tmp_path) -> None:
    service = PluginMarketplaceService(
        bot=SimpleNamespace(),
        boot=SimpleNamespace(data_dir=tmp_path),
    )

    async def first_install(*_args, **_kwargs) -> None:
        return None

    async def second_install(*_args, **_kwargs) -> None:
        return None

    service.register_installer(
        "custom",
        owner_plugin_id="plugin-a",
        install_fn=first_install,
    )
    service.register_installer(
        "custom",
        owner_plugin_id="plugin-a",
        install_fn=second_install,
    )

    assert service.get_installer("custom")["install"] is second_install
