from __future__ import annotations

import base64
import hashlib

from shinbot.agent.runtime.workflow_persistence import persist_prompt_snapshot
from shinbot.agent.services.prompt_engine.schema import PromptSnapshot
from shinbot.persistence import DatabaseManager


def test_persist_prompt_snapshot_redacts_inline_image_payloads(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    raw = b"snapshot image bytes"
    encoded = base64.b64encode(raw).decode("ascii")
    digest = hashlib.sha256(raw).hexdigest()
    snapshot = PromptSnapshot(
        id="snap-1",
        full_messages=[
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "inspect"},
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:image/png;base64,{encoded}"},
                    },
                ],
            }
        ],
    )

    persist_prompt_snapshot(db, snapshot)

    row = db.prompt_snapshots.get("snap-1")
    assert row is not None
    assert encoded not in str(row["messages"])
    assert row["messages"][0]["content"][1]["image_url"]["url"] == f"media:sha256:{digest}"
