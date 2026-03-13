"""Tests for the Looker API client."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from coupon_mention_tracker.clients.looker import (
    _NORDSEC_COUPON,
    _NORDSEC_REVENUE,
    _NORDSEC_TRANSACTIONS,
    _SAILY_COUPON,
    _SAILY_REVENUE,
    _SAILY_TRANSACTIONS,
    LookerClient,
)


@pytest.fixture
def client() -> LookerClient:
    return LookerClient(
        base_url="https://looker.test",
        client_id="test-id",
        client_secret="test-secret",  # noqa: S106
    )


@pytest.mark.asyncio
async def test_authenticate_stores_token(client) -> None:
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = {"access_token": "tok123"}

    client._http = AsyncMock()
    client._http.post = AsyncMock(return_value=mock_resp)

    await client._authenticate()

    assert client._token == "tok123"  # noqa: S105
    client._http.post.assert_called_once()


@pytest.mark.asyncio
async def test_get_coupon_performance_empty_codes(
    client,
) -> None:
    result = await client.get_coupon_performance([])
    assert result == {}


@pytest.mark.asyncio
async def test_get_coupon_performance_merges_explores(
    client,
) -> None:
    nordsec_rows = [
        {
            _NORDSEC_COUPON: "SAVE10",
            _NORDSEC_REVENUE: 500.0,
            _NORDSEC_TRANSACTIONS: 10,
        },
    ]
    saily_rows = [
        {
            _SAILY_COUPON: "SAVE10",
            _SAILY_REVENUE: 200.0,
            _SAILY_TRANSACTIONS: 5,
        },
    ]

    call_count = 0

    async def mock_query(*, model, view, fields, filters):
        nonlocal call_count
        call_count += 1
        if "nordsec" in view:
            return nordsec_rows
        return saily_rows

    client._token = "tok"  # noqa: S105
    client._run_inline_query = mock_query

    result = await client.get_coupon_performance(["SAVE10"])

    assert "SAVE10" in result
    assert result["SAVE10"].total_revenue_usd == 700.0
    assert result["SAVE10"].total_transactions == 15
    assert call_count == 2


@pytest.mark.asyncio
async def test_get_coupon_performance_graceful_on_failure(
    client,
) -> None:
    nordsec_rows = [
        {
            _NORDSEC_COUPON: "CODE1",
            _NORDSEC_REVENUE: 100.0,
            _NORDSEC_TRANSACTIONS: 3,
        },
    ]

    call_count = 0

    async def mock_query(*, model, view, fields, filters):
        nonlocal call_count
        call_count += 1
        if "esim" in view:
            raise RuntimeError("Saily API down")
        return nordsec_rows

    client._token = "tok"  # noqa: S105
    client._run_inline_query = mock_query

    result = await client.get_coupon_performance(["CODE1"])

    assert "CODE1" in result
    assert result["CODE1"].total_revenue_usd == 100.0
    assert result["CODE1"].total_transactions == 3


@pytest.mark.asyncio
async def test_parse_performance_rows_aggregates() -> None:
    rows = [
        {
            _NORDSEC_COUPON: "abc",
            _NORDSEC_REVENUE: 100.0,
            _NORDSEC_TRANSACTIONS: 2,
        },
        {
            _NORDSEC_COUPON: "ABC",
            _NORDSEC_REVENUE: 50.0,
            _NORDSEC_TRANSACTIONS: 1,
        },
    ]

    result = LookerClient._parse_performance_rows(
        rows,
        coupon_field=_NORDSEC_COUPON,
        revenue_field=_NORDSEC_REVENUE,
        transactions_field=_NORDSEC_TRANSACTIONS,
    )

    assert "ABC" in result
    assert result["ABC"].total_revenue_usd == 150.0
    assert result["ABC"].total_transactions == 3


@pytest.mark.asyncio
async def test_reauthenticates_on_401(client) -> None:
    auth_resp = MagicMock()
    auth_resp.raise_for_status = MagicMock()
    auth_resp.json.return_value = {"access_token": "new-tok"}

    first_query_resp = MagicMock()
    first_query_resp.status_code = 401

    second_query_resp = MagicMock()
    second_query_resp.status_code = 200
    second_query_resp.raise_for_status = MagicMock()
    second_query_resp.json.return_value = []

    call_idx = 0

    async def mock_post(url, **_kwargs):
        nonlocal call_idx
        call_idx += 1
        if "login" in url:
            return auth_resp
        if call_idx <= 2:
            return first_query_resp
        return second_query_resp

    client._token = "expired-tok"  # noqa: S105
    client._http = AsyncMock()
    client._http.post = mock_post

    result = await client._run_inline_query(
        model="tesonet",
        view="test_view",
        fields=["field1"],
        filters={},
    )

    assert result == []
    assert client._token == "new-tok"  # noqa: S105
