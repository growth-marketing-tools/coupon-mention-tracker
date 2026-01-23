"""Logfire-backed logging helpers with lightweight context decorators.

This module provides logging utilities that integrate with Logfire for
structured logging. IMPORTANT: Logging is NOT configured at import time.
Call `setup_logging()` explicitly from your application entrypoint (e.g.,
main.py)
before using loggers.
"""

from __future__ import annotations

import inspect
import logging
import os
import traceback
from dataclasses import dataclass
from functools import wraps
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar

import logfire


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


__all__ = [
    "async_log_with_context",
    "get_logger",
    "log_with_context",
    "setup_logger",
    "setup_logging",
]

Params = ParamSpec("Params")
ReturnType = TypeVar("ReturnType")

_logging_state = {"configured": False}


@dataclass(frozen=True)
class _CallContext:
    func: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    operation: str | None = None


def setup_logging(log_level: str | None = None) -> None:
    """Configure Logfire to emit to stderr while remaining easy to disable.

    This should be called once from the application entrypoint.
    """
    if _logging_state["configured"]:
        return

    if log_level is None:
        log_level = os.getenv("LOG_LEVEL", "INFO")

    os.environ.setdefault("LOGFIRE_SEND_TO_LOGFIRE", "false")
    os.environ.setdefault("LOGFIRE_CONSOLE", "true")
    logfire.configure()

    root_logger = logging.getLogger()

    handler_type = (
        logfire.LogfireLoggingHandler
        if isinstance(logfire.LogfireLoggingHandler, type)
        else None
    )
    has_logfire_handler = (
        any(
            isinstance(handler, handler_type)
            for handler in root_logger.handlers
        )
        if handler_type is not None
        else False
    )
    if not has_logfire_handler:
        root_logger.addHandler(logfire.LogfireLoggingHandler())

    root_logger.setLevel(_resolve_log_level(log_level))
    _logging_state["configured"] = True


def get_logger(name: str) -> logging.Logger:
    """Return a logger for the given name."""
    return logging.getLogger(name)


def log_with_context(
    **context: Any,
) -> Callable[[Callable[Params, ReturnType]], Callable[Params, ReturnType]]:
    """Inject shared logging context into synchronous call sites."""

    def decorator(
        func: Callable[Params, ReturnType],
    ) -> Callable[Params, ReturnType]:
        expects_logger = _expects_logger_arg(func)

        @wraps(func)
        def wrapper(*args: Params.args, **kwargs: Params.kwargs) -> ReturnType:
            logger_adapter = _build_logger_adapter(func, context)
            call_kwargs = dict(kwargs)
            if expects_logger:
                call_kwargs["logger"] = logger_adapter
            try:
                return func(*args, **call_kwargs)
            except Exception as error:  # pragma: no cover
                call_context = _CallContext(
                    func,
                    args,
                    call_kwargs,
                    operation=context.get("operation"),
                )
                _log_error(logger_adapter, call_context, error)
                raise

        return wrapper

    return decorator


def async_log_with_context(
    **context: Any,
) -> Callable[
    [Callable[Params, Awaitable[ReturnType]]],
    Callable[Params, Awaitable[ReturnType]],
]:
    """Async variant of log_with_context with Awaitable return annotations."""

    def decorator(
        func: Callable[Params, Awaitable[ReturnType]],
    ) -> Callable[Params, Awaitable[ReturnType]]:
        expects_logger = _expects_logger_arg(func)

        @wraps(func)
        async def wrapper(
            *args: Params.args, **kwargs: Params.kwargs
        ) -> ReturnType:
            logger_adapter = _build_logger_adapter(func, context)
            call_kwargs = dict(kwargs)
            if expects_logger:
                call_kwargs["logger"] = logger_adapter
            try:
                return await func(*args, **call_kwargs)
            except Exception as error:  # pragma: no cover
                call_context = _CallContext(
                    func,
                    args,
                    call_kwargs,
                    operation=context.get("operation"),
                )
                _log_error(logger_adapter, call_context, error)
                raise

        return wrapper

    return decorator


def setup_logger(name: str) -> logging.Logger:
    """Return a configured logger (Deprecated)."""
    return logging.getLogger(name)


def _resolve_log_level(level_name: str) -> int:
    """Translate environment log level names into logging constants."""
    numeric_level = getattr(logging, level_name.upper(), None)
    if isinstance(numeric_level, int):
        return numeric_level
    return logging.INFO


def _build_logger_adapter(
    func: Callable[..., Any], context: dict[str, Any]
) -> logging.LoggerAdapter[logging.Logger]:
    adapter_context = {
        "function": getattr(func, "__name__", "unknown"),
        "context": context,
    }
    if "operation" in context:
        adapter_context["operation"] = context["operation"]

    return logging.LoggerAdapter(
        logging.getLogger(getattr(func, "__module__", "unknown")),
        adapter_context,
    )


def _expects_logger_arg(func: Callable[..., Any]) -> bool:
    """Check if the function expects a 'logger' argument using inspection."""
    try:
        return "logger" in inspect.signature(func).parameters
    except ValueError:
        return False


def _log_error(
    logger: logging.LoggerAdapter[Any],
    call_context: _CallContext,
    error: BaseException,
) -> None:
    """Format and log an error payload."""
    filtered_kwargs = {
        key: value
        for key, value in call_context.kwargs.items()
        if key != "logger"
    }

    error_details = {
        "traceback": traceback.format_exc(),
        "args": str(call_context.args),
        "kwargs": str(filtered_kwargs),
        "function": getattr(call_context.func, "__name__", "unknown"),
        "module": getattr(call_context.func, "__module__", "unknown"),
    }

    if call_context.operation:
        error_details["operation"] = call_context.operation

    logger.error(
        "Error in %s: %s",
        getattr(call_context.func, "__name__", "unknown"),
        error,
        extra={
            "error": str(error),
            "error_type": type(error).__name__,
            "error_details": error_details,
        },
    )


logging.getLogger("coupon_mention_tracker").addHandler(logging.NullHandler())
