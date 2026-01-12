"""Logfire-backed logging helpers with lightweight context decorators.

This module provides logging utilities that integrate with Logfire for
structured logging. IMPORTANT: Logging is NOT configured at import time.
Call `setup_logging()` explicitly from your application entrypoint (e.g., main.py)
before using loggers.
"""

from __future__ import annotations

import logging
import os
import traceback
from collections.abc import Awaitable, Callable, Mapping
from functools import wraps
from types import CodeType
from typing import Any, ParamSpec, TypeVar

import logfire
from pydantic import BaseModel, ConfigDict


__all__ = [
    "ENVIRONMENT",
    "Environment",
    "async_log_with_context",
    "env",
    "get_logger",
    "log_with_context",
    "setup_logger",
    "setup_logging",
]

P = ParamSpec("P")
R = TypeVar("R")


class _LoggingState:
    """Track whether logging has been configured (avoids global statement)."""

    configured: bool = False


_logging_state = _LoggingState()


class Environment(BaseModel):
    """Snapshot of runtime environment for logging configuration.

    This model captures environment detection (Cloud Run vs local) and
    log level. Other runtime settings should be accessed via
    `config.Settings` to avoid fragmentation.

    Note: The `max_concurrent_requests`, `max_retries`, `rate_limit_requests`,
    and `rate_limit_window` fields are deprecated. Use `config.Settings` instead.
    """

    is_cloud_run: bool
    log_level: str

    model_config = ConfigDict(frozen=True)

    @classmethod
    def from_environ(cls, environ: Mapping[str, str | None]) -> Environment:
        """Build an environment snapshot from os.environ.

        This only captures environment detection and log level.
        Other settings should be read from `config.Settings`.
        """
        is_cloud_run = bool(environ.get("K_SERVICE"))
        return cls(
            is_cloud_run=is_cloud_run,
            log_level=(environ.get("LOG_LEVEL") or "INFO").upper(),
        )


def setup_logging(environment: Environment | None = None) -> None:
    """Configure Logfire to emit to stderr while remaining easy to disable.

    This function should be called once from the application entrypoint,
    NOT at module import time. It is safe to call multiple times; subsequent
    calls are no-ops.

    Args:
        environment: Optional environment configuration. If not provided,
            uses the module-level ENVIRONMENT singleton.
    """
    if _logging_state.configured:
        return

    if environment is None:
        environment = ENVIRONMENT

    os.environ.setdefault("LOGFIRE_SEND_TO_LOGFIRE", "false")
    os.environ.setdefault("LOGFIRE_CONSOLE", "true")
    logfire.configure()

    root_logger = logging.getLogger()

    # Check first before creating handler to avoid unnecessary instantiation
    has_logfire_handler = any(
        isinstance(h, logfire.LogfireLoggingHandler)
        for h in root_logger.handlers
    )
    if not has_logfire_handler:
        root_logger.addHandler(logfire.LogfireLoggingHandler())

    root_logger.setLevel(_resolve_log_level(environment.log_level))

    # Avoid leaking credentials (e.g., Slack webhook URLs) and reduce noisy
    # third-party INFO logs.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("googleapiclient.discovery_cache").setLevel(
        logging.WARNING
    )

    _logging_state.configured = True


def _build_logger_adapter(
    func: Callable[..., Any], context: dict[str, Any]
) -> logging.LoggerAdapter[logging.Logger]:
    module_name = getattr(func, "__module__", "unknown")
    func_name = getattr(func, "__name__", "unknown")
    adapter_context = {
        "function": func_name,
        "context": context,
    }
    if "operation" in context:
        adapter_context["operation"] = context["operation"]
    return logging.LoggerAdapter(
        logging.getLogger(module_name),
        adapter_context,
    )


def _expects_logger_arg(func: Callable[..., Any]) -> bool:
    code_object = getattr(func, "__code__", None)
    if not isinstance(code_object, CodeType):
        return False
    return "logger" in code_object.co_varnames[: code_object.co_argcount]


def _error_payload(
    func: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    error: BaseException,
) -> dict[str, Any]:
    filtered_kwargs = {
        key: value for key, value in kwargs.items() if key != "logger"
    }
    func_name = getattr(func, "__name__", "unknown")
    return {
        "error": str(error),
        "error_type": type(error).__name__,
        "error_details": {
            "traceback": traceback.format_exc(),
            "args": str(args),
            "kwargs": str(filtered_kwargs),
            "function": func_name,
            "module": getattr(func, "__module__", "unknown"),
        },
    }


def log_with_context(
    **context: Any,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Inject shared logging context into synchronous call sites."""

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        expects_logger = _expects_logger_arg(func)

        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            logger_adapter = _build_logger_adapter(func, context)
            call_kwargs = dict(kwargs)
            if expects_logger:
                call_kwargs["logger"] = logger_adapter
            try:
                return func(*args, **call_kwargs)
            except Exception as error:  # pragma: no cover - passthrough
                func_name = getattr(func, "__name__", "unknown")
                logger_adapter.error(
                    "Error in %s: %s",
                    func_name,
                    error,
                    extra=_error_payload(func, args, call_kwargs, error),
                )
                raise

        return wrapper

    return decorator


def async_log_with_context(
    **context: Any,
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    """Async variant of log_with_context with Awaitable return annotations."""

    def decorator(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        expects_logger = _expects_logger_arg(func)

        @wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            logger_adapter = _build_logger_adapter(func, context)
            call_kwargs = dict(kwargs)
            if expects_logger:
                call_kwargs["logger"] = logger_adapter
            try:
                return await func(*args, **call_kwargs)
            except Exception as error:  # pragma: no cover - passthrough
                payload = _error_payload(func, args, call_kwargs, error)
                payload["error_details"]["operation"] = context.get(
                    "operation", "unknown"
                )
                func_name = getattr(func, "__name__", "unknown")
                logger_adapter.error(
                    "Error in %s: %s", func_name, error, extra=payload
                )
                raise

        return wrapper

    return decorator


def _resolve_log_level(level_name: str) -> int:
    """Translate environment log level names into logging constants."""
    numeric_level = getattr(logging, level_name.upper(), None)
    if isinstance(numeric_level, int):
        return numeric_level
    return logging.INFO


def get_logger(name: str) -> logging.Logger:
    """Return a logger for the given name.

    This is the preferred way to get a logger. It returns a standard
    logging.Logger without coupling to Environment.

    Args:
        name: Logger name, typically __name__.

    Returns:
        Configured logging.Logger instance.
    """
    return logging.getLogger(name)


def setup_logger(name: str) -> tuple[logging.Logger, Environment]:
    """Return a configured logger alongside environment snapshot.

    Note: This function is provided for backwards compatibility.
    Prefer using `get_logger()` for new code.

    Args:
        name: Logger name, typically __name__.

    Returns:
        Tuple of (logger, ENVIRONMENT).
    """
    logger = logging.getLogger(name)
    logger.setLevel(_resolve_log_level(ENVIRONMENT.log_level))
    return logger, ENVIRONMENT


ENVIRONMENT: Environment = Environment.from_environ(os.environ)
env = ENVIRONMENT

# Add NullHandler to the library's root logger to prevent "No handler found"
# warnings when this package is imported by other applications that haven't
# configured logging yet. This follows Python logging best practices for libraries.
# See: https://docs.python.org/3/howto/logging.html#configuring-logging-for-a-library
logging.getLogger("coupon_mention_tracker").addHandler(logging.NullHandler())
