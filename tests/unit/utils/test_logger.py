from __future__ import annotations

import io
import logging
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from pathlib import Path

from shinbot.utils import logger as logger_utils
from shinbot.utils.log_file import parse_file_log_config


def test_third_party_noise_policy_does_not_mutate_dependency_logger_levels():
    original_levels = {
        name: logging.getLogger(name).level for name in logger_utils._NOISY_THIRD_PARTY_LOGGERS
    }
    try:
        manager = logger_utils.RuntimeLogManager()
        manager.set_third_party_noise_policy("off")

        for name in logger_utils._NOISY_THIRD_PARTY_LOGGERS:
            assert logging.getLogger(name).level == original_levels[name]
    finally:
        for name, level in original_levels.items():
            logging.getLogger(name).setLevel(level)


def test_third_party_noise_debug_policy_emits_only_at_root_debug():
    root = logging.getLogger()
    original_level = root.level
    try:
        manager = logger_utils.RuntimeLogManager()
        manager.set_third_party_noise_policy("debug")
        record = logging.LogRecord(
            name="uvicorn.error",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="WebSocket connected",
            args=(),
            exc_info=None,
        )

        root.setLevel(logging.INFO)
        assert manager.should_emit(record) is False

        root.setLevel(logging.DEBUG)
        assert manager.should_emit(record) is True
    finally:
        root.setLevel(original_level)


def test_third_party_warning_is_not_treated_as_noise():
    manager = logger_utils.RuntimeLogManager()
    manager.set_third_party_noise_policy("off")
    record = logging.LogRecord(
        name="websockets.server",
        level=logging.WARNING,
        pathname=__file__,
        lineno=1,
        msg="connection issue",
        args=(),
        exc_info=None,
    )

    assert manager.should_emit(record) is True


def test_known_low_value_third_party_warning_can_be_suppressed():
    manager = logger_utils.RuntimeLogManager()
    manager.set_third_party_noise_policy("off")
    record = logging.LogRecord(
        name="LiteLLM",
        level=logging.WARNING,
        pathname=__file__,
        lineno=1,
        msg="litellm: could not pre-load bedrock-runtime response stream shape",
        args=(),
        exc_info=None,
    )

    assert manager.should_emit(record) is False


def test_openai_transport_info_is_treated_as_noise():
    manager = logger_utils.RuntimeLogManager()
    manager.set_third_party_noise_policy("off")
    record = logging.LogRecord(
        name="httpcore.connection",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="connect_tcp.started",
        args=(),
        exc_info=None,
    )

    assert manager.should_emit(record) is False


def test_runtime_log_manager_snapshot_exposes_state_and_sources():
    manager = logger_utils.RuntimeLogManager()
    manager.register_source("tests.snapshot", "测试源", color="bright-blue")

    snapshot = manager.snapshot()

    assert snapshot["thirdPartyNoise"] == "off"
    assert snapshot["sourceWidth"] == 20
    assert "DEBUG" in snapshot["availableLevels"]
    assert "debug" in snapshot["availableThirdPartyNoise"]
    assert "bright_blue" in snapshot["availableColors"]
    assert {
        "loggerName": "tests.snapshot",
        "source": "测试源",
        "color": "bright_blue",
    } in snapshot["sources"]


def test_parse_file_log_config_defaults_to_size_rotated_data_log():
    config = parse_file_log_config(None)

    assert config.enabled is True
    assert config.path == Path("logs/shinbot.log")
    assert config.when == "midnight"
    assert config.backup_count == 14
    assert config.max_bytes == 10 * 1024 * 1024


def test_parse_file_log_config_accepts_hourly_alias_and_disable():
    config = parse_file_log_config(
        {
            "enabled": "true",
            "path": "logs/debug.log",
            "when": "hourly",
            "interval": "2",
            "backup_count": "3",
            "max_bytes": "2048",
        }
    )

    assert config.enabled is True
    assert config.path == Path("logs/debug.log")
    assert config.when == "H"
    assert config.interval == 2
    assert config.backup_count == 3
    assert config.max_bytes == 2048
    assert parse_file_log_config(False).enabled is False


def test_runtime_log_manager_configures_size_rotating_file_handler(tmp_path: Path):
    manager = logger_utils.RuntimeLogManager()
    root = logging.getLogger()
    previous_handlers = list(root.handlers)
    previous_level = root.level
    root.handlers = []
    root.setLevel(logging.INFO)
    try:
        manager.configure_file_handler(parse_file_log_config(None), data_dir=tmp_path)
        handlers = list(manager.iter_file_handlers(root))

        assert len(handlers) == 1
        assert isinstance(handlers[0], RotatingFileHandler)
        assert Path(handlers[0].baseFilename) == tmp_path / "logs" / "shinbot.log"

        logging.getLogger("test.file-log").info("persist me")
        handlers[0].flush()
        assert "persist me" in (tmp_path / "logs" / "shinbot.log").read_text(encoding="utf-8")

        manager.configure_file_handler(parse_file_log_config(False), data_dir=tmp_path)
        assert list(manager.iter_file_handlers(root)) == []
    finally:
        for handler in list(root.handlers):
            root.removeHandler(handler)
            handler.close()
        root.handlers = previous_handlers
        root.setLevel(previous_level)


def test_runtime_log_manager_supports_time_only_file_rotation(tmp_path: Path):
    manager = logger_utils.RuntimeLogManager()
    root = logging.getLogger()
    previous_handlers = list(root.handlers)
    previous_level = root.level
    root.handlers = []
    root.setLevel(logging.INFO)
    try:
        manager.configure_file_handler(
            parse_file_log_config({"max_bytes": 0, "when": "hourly"}),
            data_dir=tmp_path,
        )
        handlers = list(manager.iter_file_handlers(root))

        assert len(handlers) == 1
        assert isinstance(handlers[0], TimedRotatingFileHandler)
    finally:
        for handler in list(root.handlers):
            root.removeHandler(handler)
            handler.close()
        root.handlers = previous_handlers
        root.setLevel(previous_level)


def test_runtime_log_manager_apply_runtime_config_validates_values():
    manager = logger_utils.RuntimeLogManager()
    manager.apply_runtime_config(third_party_noise="on")
    assert manager.third_party_noise_policy() == "on"

    try:
        manager.apply_runtime_config(third_party_noise="loud")
    except ValueError as exc:
        assert "Unsupported third-party noise policy" in str(exc)
    else:
        raise AssertionError("Expected invalid third-party noise policy to fail")


def test_normalize_log_level_aliases_warning_and_critical():
    assert logger_utils.normalize_log_level("WARNING") == "WARN"
    assert logger_utils.normalize_log_level("CRITICAL") == "ERROR"
    assert logger_utils.normalize_log_level("info") == "INFO"


def test_display_log_level_preserves_app_info_logs():
    record = logging.LogRecord(
        name="shinbot.api.app",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="Management API ready",
        args=(),
        exc_info=None,
    )

    assert logger_utils.display_log_level(record) == "INFO"


def test_shorten_logger_name_strips_prefix_and_keeps_tail_parts():
    assert (
        logger_utils.shorten_logger_name("shinbot.core.application.boot") == "core.application.boot"
    )
    assert (
        logger_utils.shorten_logger_name("shinbot.api.routers.model_runtime")
        == "api.routers.model_runtime"
    )
    assert logger_utils.shorten_logger_name("uvicorn.access") == "uvicorn.access"


def test_format_console_source_pads_and_truncates_to_fixed_width():
    short = logger_utils.format_console_source("boot", width=12)
    long = logger_utils.format_console_source("adapter:shinbot_adapter_onebot_v11", width=20)
    chinese = logger_utils.format_console_source("核心启动", width=10)

    assert short == "boot        "
    assert len(short) == 12
    assert long.startswith("...")
    assert long.endswith("onebot_v11")
    assert logger_utils._display_width(long) == 20
    assert chinese == "核心启动  "
    assert logger_utils._display_width(chinese) == 10


def test_runtime_log_manager_uses_explicit_record_source():
    manager = logger_utils.RuntimeLogManager()
    record = logging.LogRecord(
        name="shinbot.core.application.boot",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="Boot",
        args=(),
        exc_info=None,
    )
    record.shinbot_source = "boot"

    assert manager.record_source(record) == "boot"


def test_runtime_log_manager_uses_registered_longest_prefix_source():
    manager = logger_utils.RuntimeLogManager()
    manager.register_source("shinbot.core", "core")
    manager.register_source("shinbot.core.application", "boot")
    record = logging.LogRecord(
        name="shinbot.core.application.boot",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="Boot",
        args=(),
        exc_info=None,
    )

    assert manager.record_source(record) == "boot"


def test_get_logger_provider_can_declare_display_source():
    log = logger_utils.get_logger("tests.logger.provider", source="provider", color="cyan")
    record = logging.LogRecord(
        name=f"{log.name}.child",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="Provider log",
        args=(),
        exc_info=None,
    )

    assert logger_utils.log_record_source(record) == "provider"
    assert logger_utils.runtime_log_manager.record_source_color(record) == "cyan"


def test_console_handler_keeps_message_column_aligned():
    stream = io.StringIO()
    handler = logger_utils.build_console_handler(
        logging.DEBUG,
        stream=stream,
        use_color=False,
    )
    logger = logging.getLogger("test.console-format")
    previous_propagate = logger.propagate
    previous_handlers = list(logger.handlers)
    logger.handlers = [handler]
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    try:
        logger.info("short")
        logger.warning("long")
    finally:
        logger.handlers = previous_handlers
        logger.propagate = previous_propagate

    lines = [line for line in stream.getvalue().splitlines() if line]
    assert len(lines) == 2
    assert [line.index("|") for line in lines] == [36, 36]
    assert lines[0].endswith("| short")
    assert lines[1].endswith("| long")


def test_console_handler_aligns_multiline_messages():
    stream = io.StringIO()
    handler = logger_utils.build_console_handler(
        logging.DEBUG,
        stream=stream,
        use_color=False,
    )
    logger = logging.getLogger("test.console-multiline")
    previous_propagate = logger.propagate
    previous_handlers = list(logger.handlers)
    logger.handlers = [handler]
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    try:
        logger.info("first\nsecond\n第三行")
    finally:
        logger.handlers = previous_handlers
        logger.propagate = previous_propagate

    lines = stream.getvalue().splitlines()
    assert len(lines) == 3
    assert lines[0].endswith("| first")
    assert lines[1] == (" " * 38) + "second"
    assert lines[2] == (" " * 38) + "第三行"


def test_console_handler_colors_registered_source_only_when_color_enabled():
    stream = io.StringIO()
    logger_utils.register_log_source("test.console-color", "彩色源", color="red")
    handler = logger_utils.build_console_handler(
        logging.DEBUG,
        stream=stream,
        use_color=True,
    )
    logger = logging.getLogger("test.console-color")
    previous_propagate = logger.propagate
    previous_handlers = list(logger.handlers)
    logger.handlers = [handler]
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    try:
        logger.info("hello")
    finally:
        logger.handlers = previous_handlers
        logger.propagate = previous_propagate

    output = stream.getvalue()
    assert "\033[31m彩色源" in output
    assert output.endswith("| hello\n")


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


def test_parse_log_event_extracts_event_and_fields():
    message = logger_utils.format_log_event(
        "agent.signal.decision",
        trace_id="ingress:bot:msg-1",
        session_id="bot:group:room",
        accepted=True,
        targets=["agent_entry"],
    )

    parsed = logger_utils.parse_log_event(message)

    assert parsed == {
        "event": "agent.signal.decision",
        "fields": {
            "trace_id": "ingress:bot:msg-1",
            "session_id": "bot:group:room",
            "accepted": True,
            "targets": ["agent_entry"],
        },
    }
