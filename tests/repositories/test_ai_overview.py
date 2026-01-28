"""Unit tests for AIOverviewRepository (no real database connections)."""

from __future__ import annotations

from datetime import date, datetime
from typing import cast
from unittest.mock import MagicMock
from uuid import uuid4

import asyncpg
import pytest

from coupon_mention_tracker.repositories.ai_overview import (
    AIOverviewRepository,
)


class _FakeConn:
    def __init__(self, rows):
        self._rows = rows
        self.calls = []

    async def fetch(self, query, *params):
        self.calls.append((query, params))
        return self._rows


class _AcquireCtx:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeConn:
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class _FakePool:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn
        self.closed = False

    def acquire(self) -> _AcquireCtx:
        return _AcquireCtx(self._conn)

    async def close(self) -> None:
        self.closed = True


def _make_mock_settings(
    database_url: str = "postgresql://user:pass@localhost/db",
):
    """Create a mock Settings object for testing."""
    settings = MagicMock()
    settings.database_url_str = database_url
    return settings


@pytest.mark.asyncio
async def test_acquire_raises_when_not_connected() -> None:
    repo = AIOverviewRepository(_make_mock_settings())

    with pytest.raises(RuntimeError, match="Database not connected"):
        async with repo.acquire():
            pass


@pytest.mark.asyncio
async def test_connect_and_disconnect_use_pool(monkeypatch) -> None:
    created = {}

    async def _connect(self):
        created["url"] = self._settings.database_url_str
        self._pool = _FakePool(_FakeConn([]))
        return self._pool

    monkeypatch.setattr(
        "coupon_mention_tracker.clients.database.DatabaseClient.connect",
        _connect,
    )

    settings = _make_mock_settings("postgresql://example")
    repo = AIOverviewRepository(settings)
    await repo.connect()
    assert created["url"] == "postgresql://example"

    pool = cast(_FakePool, repo.pool)
    assert pool is not None

    await repo.disconnect()
    assert pool.closed is True
    assert repo.pool is None


@pytest.mark.asyncio
async def test_get_prompts_maps_rows_to_models() -> None:
    now = datetime(2026, 1, 1, 0, 0, 0)
    rows = [
        {
            "id": uuid4(),
            "prompt_text": "nordvpn coupon",
            "primary_product": "nordvpn",
            "location": "US",
            "status": "active",
            "tags": ["t"],
            "created_at": now,
        }
    ]
    conn = _FakeConn(rows)
    repo = AIOverviewRepository(_make_mock_settings())
    fake_pool = _FakePool(conn)
    repo._db_client._pool = cast(asyncpg.Pool, fake_pool)

    prompts = await repo.get_prompts(product="nordvpn", location="US")

    assert len(prompts) == 1
    assert prompts[0].prompt_text == "nordvpn coupon"
    assert conn.calls


@pytest.mark.asyncio
async def test_get_results_for_period_maps_rows_to_models() -> None:
    prompt_id = uuid4()
    result_id = uuid4()
    scraped = date(2026, 1, 2)
    rows = [
        {
            "prompt_id": prompt_id,
            "prompt_text": "k",
            "primary_product": "p",
            "location": None,
            "status": "active",
            "tags": None,
            "prompt_created_at": None,
            "result_id": result_id,
            "provider": "google",
            "scraped_date": scraped,
            "scraped_at": None,
            "response_text": "text",
            "sources": None,
            "ahrefs_volume": None,
            "sentiment_label": None,
        }
    ]

    conn = _FakeConn(rows)
    repo = AIOverviewRepository(_make_mock_settings())
    fake_pool = _FakePool(conn)
    repo._db_client._pool = cast(asyncpg.Pool, fake_pool)

    results = await repo.get_results_for_period(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 7),
        provider="google",
    )

    assert len(results) == 1
    prompt, result = results[0]
    assert prompt.id == prompt_id
    assert result.id == result_id
    assert result.scraped_date == scraped


@pytest.mark.asyncio
async def test_get_results_last_n_days_delegates_to_period(monkeypatch) -> None:
    repo = AIOverviewRepository(_make_mock_settings())

    called = {}

    async def _fake_get_results_for_period(
        start_date, end_date, provider, tags=None
    ):
        called["start_date"] = start_date
        called["end_date"] = end_date
        called["provider"] = provider
        called["tags"] = tags
        return []

    monkeypatch.setattr(
        repo, "get_results_for_period", _fake_get_results_for_period
    )

    await repo.get_results_last_n_days(days=3, provider="google")

    assert called["provider"] == "google"
    assert called["end_date"] == date.today()
    assert called["tags"] is None
