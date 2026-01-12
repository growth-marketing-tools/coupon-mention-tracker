"""Unit tests for logging utilities."""

from __future__ import annotations

import asyncio
import logging

import pytest

from coupon_mention_tracker.core import logger as logger_module


def test_environment_from_environ_detects_cloud_run_and_level() -> None:
    env = logger_module.Environment.from_environ(
        {"K_SERVICE": "svc", "LOG_LEVEL": "debug"}
    )
    assert env.is_cloud_run is True
    assert env.log_level == "DEBUG"


def test_resolve_log_level_unknown_defaults_info() -> None:
    assert logger_module._resolve_log_level("nope") == logging.INFO


def test_expects_logger_arg() -> None:
    def f(x, logger):
        _ = logger
        return x

    def g(x):
        return x

    assert logger_module._expects_logger_arg(f) is True
    assert logger_module._expects_logger_arg(g) is False


def test_log_with_context_injects_logger_adapter() -> None:
    @logger_module.log_with_context(operation="op")
    def f(x: int, logger):
        assert isinstance(logger, logging.LoggerAdapter)
        return (x, logger.extra["operation"])

    assert f(1) == (1, "op")


@pytest.mark.asyncio
async def test_async_log_with_context_injects_logger_adapter() -> None:
    @logger_module.async_log_with_context(operation="op")
    async def f(x: int, logger):
        await asyncio.sleep(0)
        assert isinstance(logger, logging.LoggerAdapter)
        return (x, logger.extra["operation"])

    assert await f(1) == (1, "op")


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

    logging.getLogger("httpx").setLevel(logging.NOTSET)
    logging.getLogger("httpcore").setLevel(logging.NOTSET)
    logging.getLogger("googleapiclient.discovery_cache").setLevel(
        logging.NOTSET
    )

    logger_module._logging_state.configured = False
    logger_module.setup_logging(
        logger_module.Environment(is_cloud_run=False, log_level="INFO")
    )
    logger_module.setup_logging(
        logger_module.Environment(is_cloud_run=False, log_level="INFO")
    )

    assert calls["configure"] == 1

    assert logging.getLogger("httpx").level == logging.WARNING
    assert logging.getLogger("httpcore").level == logging.WARNING
    assert (
        logging.getLogger("googleapiclient.discovery_cache").level
        == logging.WARNING
    )


def test_get_logger_and_setup_logger() -> None:
    log = logger_module.get_logger("x")
    assert isinstance(log, logging.Logger)

    log2, env = logger_module.setup_logger("y")
    assert isinstance(log2, logging.Logger)
    assert env.log_level
