"""Tests for configuration settings."""

from __future__ import annotations

import pytest

from coupon_mention_tracker.core.config import Settings, get_settings


def test_database_url_str_computed_field() -> None:
    settings = Settings(
        database_url="postgresql://user:pass@localhost:5432/db",
        slack_webhook_url="https://example.invalid",
        google_workspace_credentials="{}",
    )
    assert (
        settings.database_url_str == "postgresql://user:pass@localhost:5432/db"
    )


def test_database_url_auto_encodes_special_chars() -> None:
    # Password with special chars: ^h4bj!PXP!kc6K3jE3huQ%&wUG3^ze
    settings = Settings(
        database_url="postgresql://user:^pass!word%&@localhost:5432/db",
        slack_webhook_url="https://example.invalid",
        google_workspace_credentials="{}",
    )
    # Special chars should be URL-encoded
    assert "%5E" in settings.database_url_str  # ^
    assert "%21" in settings.database_url_str  # !
    assert "%25" in settings.database_url_str  # %
    assert "%26" in settings.database_url_str  # &


def test_database_url_handles_already_encoded_passwords() -> None:
    """Test that already URL-encoded passwords are handled correctly."""
    # Password already URL-encoded: %5Eh4bj%21PXP%21kc6K3jE3huQ%25%26wUG3%5Eze
    settings = Settings(
        database_url="postgresql://user:%5Eh4bj%21PXP@localhost:5432/db",
        slack_webhook_url="https://example.invalid",
        google_workspace_credentials="{}",
    )
    # Should not double-encode (should decode then re-encode to same result)
    assert "%5E" in settings.database_url_str  # ^
    assert "%21" in settings.database_url_str  # !
    # Should not have double-encoded patterns like %255E
    assert "%25" not in settings.database_url_str


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
