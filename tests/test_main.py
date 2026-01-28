"""Unit tests for the main entrypoint logic (without external services)."""

from __future__ import annotations

import pytest

from coupon_mention_tracker import main
from coupon_mention_tracker.core.config import Settings


def test_fetch_coupons_from_google_sheets_uses_settings(monkeypatch) -> None:
    calls = {}

    class _Client:
        def __init__(self, spreadsheet_id: str, credentials_json: str) -> None:
            calls["spreadsheet_id"] = spreadsheet_id
            calls["credentials_json"] = credentials_json

        def get_coupons(
            self, gid: int, column_name: str = "Coupon"
        ) -> list[str]:
            calls["gid"] = gid
            calls["column_name"] = column_name
            return ["SAVE10", "NEW20"]

    monkeypatch.setattr(main, "GoogleSheetsClient", _Client)

    settings = Settings(
        database_url="postgresql://test:test@localhost:5432/test",
        slack_webhook_url="https://hooks.slack.com/test",
        google_sheets_spreadsheet_id="sheet",
        google_workspace_credentials="{}",
        google_sheets_coupon_gid=123,
        google_sheets_coupon_column="Coupon",
    )

    coupons = main.fetch_coupons_from_google_sheets(settings)

    assert coupons == ["SAVE10", "NEW20"]
    assert calls["spreadsheet_id"] == "sheet"
    assert calls["gid"] == 123


@pytest.mark.asyncio
async def test_run_weekly_report_disconnects_repository_on_connect_failure(
    monkeypatch,
) -> None:
    class _Repo:
        def __init__(self, database_url: str) -> None:
            self.database_url = database_url
            self.disconnected = False

        async def connect(self) -> None:
            raise RuntimeError("boom")

        async def disconnect(self) -> None:
            self.disconnected = True

    settings = type(
        "S",
        (),
        {
            "database_url_str": "postgresql://example",
            "slack_webhook_url": "https://example.invalid",
            "slack_channel": "#x",
            "report_lookback_days": 7,
        },
    )()

    monkeypatch.setattr(main, "get_settings", lambda: settings)
    monkeypatch.setattr(
        main, "fetch_coupons_from_google_sheets", lambda _s: ["SAVE10"]
    )

    created = {}

    def _repo_factory(url: str) -> _Repo:
        repo = _Repo(url)
        created["repo"] = repo
        return repo

    monkeypatch.setattr(main, "AIOverviewRepository", _repo_factory)

    code = await main.run_weekly_report(days=7, send_slack=False)

    assert code == 1
    assert created["repo"].disconnected is True
