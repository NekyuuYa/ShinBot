from __future__ import annotations

import logging

from shinbot.utils import logger as logger_utils


def test_configure_third_party_loggers_clamps_debug_to_info():
    original_levels = {
        name: logging.getLogger(name).level for name in logger_utils._NOISY_THIRD_PARTY_LOGGERS
    }
    try:
        logger_utils._configure_third_party_loggers(logging.DEBUG)

        for name in logger_utils._NOISY_THIRD_PARTY_LOGGERS:
            assert logging.getLogger(name).level == logging.INFO
    finally:
        for name, level in original_levels.items():
            logging.getLogger(name).setLevel(level)


def test_configure_third_party_loggers_preserves_higher_root_level():
    original_levels = {
        name: logging.getLogger(name).level for name in logger_utils._NOISY_THIRD_PARTY_LOGGERS
    }
    try:
        logger_utils._configure_third_party_loggers(logging.ERROR)

        for name in logger_utils._NOISY_THIRD_PARTY_LOGGERS:
            assert logging.getLogger(name).level == logging.ERROR
    finally:
        for name, level in original_levels.items():
            logging.getLogger(name).setLevel(level)


def test_normalize_log_level_aliases_warning_and_critical():
    assert logger_utils.normalize_log_level("WARNING") == "WARN"
    assert logger_utils.normalize_log_level("CRITICAL") == "ERROR"
    assert logger_utils.normalize_log_level("info") == "INFO"


def test_shorten_logger_name_strips_prefix_and_keeps_tail_parts():
    assert (
        logger_utils.shorten_logger_name("shinbot.core.application.boot") == "core.application.boot"
    )
    assert (
        logger_utils.shorten_logger_name("shinbot.api.routers.model_runtime")
        == "api.routers.model_runtime"
    )
    assert logger_utils.shorten_logger_name("uvicorn.access") == "uvicorn.access"


def test_format_log_event_omits_empty_fields_and_serializes_collections():
    message = logger_utils.format_log_event(
        "audit.command",
        command="ping",
        ok=True,
        metadata={"channel": "group"},
        tags=["core", "health"],
        empty="",
        missing=None,
    )

    assert message.startswith("audit.command")
    assert "command=ping" in message
    assert "ok=true" in message
    assert 'metadata={"channel":"group"}' in message
    assert 'tags=["core","health"]' in message
    assert "empty=" not in message
    assert "missing=" not in message
