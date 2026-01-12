"""Tests for configuration settings."""

from __future__ import annotations

import pytest
from pydantic import PostgresDsn

from coupon_mention_tracker.core.config import Settings, get_settings


def test_database_url_str_computed_field() -> None:
    settings = Settings(
        database_url=PostgresDsn("postgresql://user:pass@localhost:5432/db"),
        slack_webhook_url="https://example.invalid",
        google_workspace_credentials="{}",
    )
    assert settings.database_url_str.startswith("postgresql://")


def test_get_settings_is_cached(monkeypatch) -> None:
    get_settings.cache_clear()

    monkeypatch.setenv(
        "DATABASE_URL", "postgresql://user:pass@localhost:5432/db"
    )
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://example.invalid")
    monkeypatch.setenv("GOOGLE_WORKSPACE_CREDENTIALS", "{}")

    first = get_settings()
    second = get_settings()

    assert first is second


@pytest.fixture(autouse=True)
def _clear_settings_cache() -> None:
    get_settings.cache_clear()
