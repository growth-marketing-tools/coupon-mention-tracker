"""Unit tests for logging utilities."""

from __future__ import annotations

from coupon_mention_tracker.core import logger as logger_module


def test_setup_logging_is_idempotent(monkeypatch) -> None:
    sink_adds = {"count": 0}
    original_add = logger_module.logger.add

    def _counting_add(*args, **kwargs):
        sink_adds["count"] += 1
        return original_add(*args, **kwargs)

    monkeypatch.setattr(logger_module.logger, "add", _counting_add)
    logger_module._logging_state["configured"] = False

    logger_module.setup_logging("INFO")
    first_count = sink_adds["count"]

    logger_module.setup_logging("INFO")
    assert sink_adds["count"] == first_count  # no additional add on 2nd call
