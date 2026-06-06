from __future__ import annotations

import json

import pytest

from shinbot.admin.plugin_install import (
    PluginInstallError,
    PluginInstallManifest,
    PluginInstallRecord,
)


def test_plugin_install_manifest_round_trips_records(tmp_path):
    manifest = PluginInstallManifest(tmp_path)
    record = PluginInstallRecord(
        plugin_id="shinbot_plugin_demo",
        source_type="github",
        source_url="https://github.com/NekyuuYa/shinbot-plugin-demo",
        ref="main",
        resolved_ref="abc123",
        installed_at=1.0,
        updated_at=2.0,
        installed_version="0.1.0",
        archive_sha256="hash",
    )

    manifest.save({"shinbot_plugin_demo": record})

    loaded = manifest.load()
    assert loaded["shinbot_plugin_demo"].as_dict() == record.as_dict()
    payload = json.loads((tmp_path / "plugin_install_manifest.json").read_text(encoding="utf-8"))
    assert payload["schema_version"] == 1
    assert payload["plugins"]["shinbot_plugin_demo"]["managed_by_webui"] is True


def test_plugin_install_manifest_rejects_invalid_json(tmp_path):
    (tmp_path / "plugin_install_manifest.json").write_text("{", encoding="utf-8")
    manifest = PluginInstallManifest(tmp_path)

    with pytest.raises(PluginInstallError) as exc_info:
        manifest.load()

    assert exc_info.value.code == "PLUGIN_INSTALL_MANIFEST_INVALID"
