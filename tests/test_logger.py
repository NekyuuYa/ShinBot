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
