"""Loguru-backed logging configuration.

Call ``setup_logging()`` once from the application entrypoint before
using loggers.  Every other module should import the logger directly::

    from loguru import logger
"""

from __future__ import annotations

import os
import sys

from loguru import logger


__all__ = ["setup_logging"]

_LOG_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> "
    "<level>{level: <8}</level> "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> "
    "<level>{message}</level>"
)

_logging_state = {"configured": False}


def setup_logging(log_level: str | None = None) -> None:
    """Configure loguru to emit to stderr.

    This should be called once from the application entrypoint.
    """
    if _logging_state["configured"]:
        return

    if log_level is None:
        log_level = os.getenv("LOG_LEVEL", "INFO")

    logger.remove()
    logger.add(
        sys.stderr,
        format=_LOG_FORMAT,
        level=log_level.upper(),
        backtrace=False,
        diagnose=False,
        enqueue=True,
        colorize=sys.stderr.isatty(),
    )

    _logging_state["configured"] = True
