from __future__ import annotations

import json

from shinbot.persistence import DatabaseManager
from shinbot.utils.astrbot_import import import_astrbot_export


def test_import_astrbot_platform_message_history_shinku_only(tmp_path):
    export_path = tmp_path / "astrbot.json"
    export_path.write_text(
        json.dumps(
            {
                "platform_message_history": [
                    {
                        "id": 297,
                        "user_id": "332376242",
                        "sender_name": "事实上，你所爱上的我的人格是子虚乌有的。",
                        "content": [
                            {
                                "type": "reply",
                                "data": {
                                    "id": "588972178",
                                    "text": "……这种问题，你是认真的吗？请适可而止吧",
                                },
                            },
                            {
                                "type": "at",
                                "data": {
                                    "qq": "3575371140",
                                },
                            },
                            {
                                "type": "text",
                                "data": {
                                    "text": "这是日本最有名的动漫里面的名台词，你不要逃避",
                                },
                            },
                        ],
                        "created_at": "2026-03-28T15:34:31.447589",
                        "updated_at": "2026-03-28T15:34:31.447604",
                        "platform_id": "Shinku",
                        "sender_id": "3224963079",
                    },
                    {
                        "id": 298,
                        "user_id": "332376242",
                        "sender_name": "AstrBot",
                        "content": [
                            {
                                "type": "poke",
                                "data": {
                                    "id": "3224963079",
                                    "type": "poke",
                                },
                            },
                        ],
                        "created_at": "2026-03-28T15:34:40.000000",
                        "updated_at": "2026-03-28T15:34:40.000010",
                        "platform_id": "Shinku",
                        "sender_id": "3575371140",
                    },
                    {
                        "id": 299,
                        "user_id": "332376242",
                        "sender_name": "去星星上挖点寄生虫",
                        "content": [
                            {
                                "type": "image",
                                "data": {
                                    "url": "https://example.com/a.png",
                                },
                            }
                        ],
                        "created_at": "2026-03-28T15:34:50.000000",
                        "updated_at": "2026-03-28T15:34:50.000010",
                        "platform_id": "Shinku",
                        "sender_id": "3291948435",
                    },
                    {
                        "id": 300,
                        "user_id": "5a84ee5b-e8f4-4529-b2c4-cf5c6f191d62",
                        "sender_name": "Nekyuu",
                        "content": {
                            "type": "user",
                            "message": [
                                {
                                    "type": "plain",
                                    "text": "webchat row should be ignored",
                                }
                            ],
                        },
                        "created_at": "2026-03-26T15:10:43.936254",
                        "updated_at": "2026-03-26T15:10:43.936268",
                        "platform_id": "webchat",
                        "sender_id": "Nekyuu",
                    },
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    stats = import_astrbot_export(json_path=export_path, data_dir=tmp_path)

    assert stats.rows_seen == 4
    assert stats.rows_filtered == 1
    assert stats.sessions_upserted == 1
    assert stats.messages_inserted == 2
    assert stats.messages_skipped == 1
    assert stats.duplicate_messages == 0

    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()

    rows = db.message_logs.get_recent("onebot_v11:group:332376242:332376242", limit=10)
    assert len(rows) == 2

    first = rows[0]
    assert first["role"] == "user"
    assert first["sender_id"] == "3224963079"
    assert first["is_mentioned"] == 1
    assert first["raw_text"] == "这是日本最有名的动漫里面的名台词，你不要逃避"
    first_content = json.loads(first["content_json"])
    assert first_content == [
        {"type": "quote", "attrs": {"id": "588972178"}, "children": []},
        {"type": "at", "attrs": {"id": "3575371140"}, "children": []},
        {
            "type": "text",
            "attrs": {"content": "这是日本最有名的动漫里面的名台词，你不要逃避"},
            "children": [],
        },
    ]

    second = rows[1]
    assert second["role"] == "assistant"
    assert second["sender_id"] == "3575371140"
    assert second["raw_text"] == ""
    assert json.loads(second["content_json"]) == [
        {
            "type": "sb:poke",
            "attrs": {"target": "3224963079", "type": "poke"},
            "children": [],
        }
    ]

    again = import_astrbot_export(json_path=export_path, data_dir=tmp_path)
    assert again.messages_inserted == 0
    assert again.duplicate_messages == 2
