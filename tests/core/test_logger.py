"""Unit tests for logging utilities."""

from __future__ import annotations

import asyncio
import logging

import pytest

from coupon_mention_tracker.core import logger as logger_module


def test_resolve_log_level_unknown_defaults_info() -> None:
    assert logger_module._resolve_log_level("nope") == logging.INFO


def test_expects_logger_arg() -> None:
    def func_with_logger(x, logger):
        _ = logger
        return x

    def func_without_logger(x):
        return x

    assert logger_module._expects_logger_arg(func_with_logger) is True
    assert logger_module._expects_logger_arg(func_without_logger) is False


def test_log_with_context_injects_logger_adapter() -> None:
    @logger_module.log_with_context(operation="op")
    def func_to_log(x: int, logger):
        assert isinstance(logger, logging.LoggerAdapter)
        return (x, logger.extra["operation"])

    assert func_to_log(1) == (1, "op")


@pytest.mark.asyncio
async def test_async_log_with_context_injects_logger_adapter() -> None:
    @logger_module.async_log_with_context(operation="op")
    async def async_func_to_log(x: int, logger):
        await asyncio.sleep(0)
        assert isinstance(logger, logging.LoggerAdapter)
        return (x, logger.extra["operation"])

    assert await async_func_to_log(1) == (1, "op")


def test_setup_logging_is_idempotent(monkeypatch) -> None:
    calls = {"configure": 0}

    class _Handler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            _ = record

    def _configure():
        calls["configure"] += 1

    monkeypatch.setattr(logger_module.logfire, "configure", _configure)
    monkeypatch.setattr(
        logger_module.logfire, "LogfireLoggingHandler", _Handler
    )

    logger_module._logging_state["configured"] = False
    logger_module.setup_logging("INFO")
    logger_module.setup_logging("INFO")

    assert calls["configure"] == 1


def test_get_logger_and_setup_logger() -> None:
    log = logger_module.get_logger("x")
    assert isinstance(log, logging.Logger)

    log2 = logger_module.setup_logger("y")
    assert isinstance(log2, logging.Logger)
