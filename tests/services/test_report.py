"""Tests for weekly report generation."""

from __future__ import annotations

from datetime import date
from uuid import uuid4

import pytest

from coupon_mention_tracker.clients.slack_client import SlackNotifier
from coupon_mention_tracker.core.models import (
    AIOverviewPrompt,
    AIOverviewResult,
)
from coupon_mention_tracker.services.coupon_matcher import CouponMatcher
from coupon_mention_tracker.services.report import WeeklyReportGenerator


class _FakeRepo:
    def __init__(self, results):
        self._results = results

    async def get_results_last_n_days(self, days: int = 7):
        _ = days
        return self._results


class _FakeNotifier(SlackNotifier):
    def __init__(self) -> None:
        super().__init__(webhook_url="https://example.invalid")
        self.sent = []
        self._return_value = True

    async def send_weekly_report(self, rows, start_date, end_date):
        self.sent.append((rows, start_date, end_date))
        return self._return_value


@pytest.mark.asyncio
async def test_generate_report_builds_rows_and_matches() -> None:
    prompt1 = AIOverviewPrompt(
        id=uuid4(),
        prompt_text="nordvpn coupon",
        primary_product="nordvpn",
        location="US",
    )
    prompt2 = AIOverviewPrompt(
        id=uuid4(),
        prompt_text="nordpass pricing",
        primary_product="nordpass",
        location=None,
    )

    result1 = AIOverviewResult(
        id=uuid4(),
        prompt_id=prompt1.id,
        provider="google",
        scraped_date=date(2026, 1, 1),
        response_text="Use SAVE10 today",
    )
    result2 = AIOverviewResult(
        id=uuid4(),
        prompt_id=prompt1.id,
        provider="google",
        scraped_date=date(2026, 1, 2),
        response_text="SAVE10 still works",
    )
    result3 = AIOverviewResult(
        id=uuid4(),
        prompt_id=prompt2.id,
        provider="google",
        scraped_date=date(2026, 1, 2),
        response_text="No coupons here",
    )

    repo = _FakeRepo(
        [(prompt1, result1), (prompt1, result2), (prompt2, result3)]
    )
    matcher = CouponMatcher(["SAVE10"])
    notifier = _FakeNotifier()
    generator = WeeklyReportGenerator(repo, matcher, notifier)

    rows, matches = await generator.generate_report(days=7)

    assert len(matches) == 2

    row_by_key = {(row.keyword, row.coupon_detected): row for row in rows}
    match_row = row_by_key[("nordvpn coupon", "SAVE10")]
    assert match_row.is_valid_coupon is True
    assert match_row.mention_count == 2
    assert match_row.last_seen == date(2026, 1, 2)

    empty_row = row_by_key[("nordpass pricing", None)]
    assert empty_row.is_valid_coupon is None


@pytest.mark.asyncio
async def test_run_and_send_calls_notifier() -> None:
    prompt = AIOverviewPrompt(
        id=uuid4(),
        prompt_text="nordvpn coupon",
        primary_product="nordvpn",
        location=None,
    )
    result = AIOverviewResult(
        id=uuid4(),
        prompt_id=prompt.id,
        provider="google",
        scraped_date=date.today(),
        response_text="Use SAVE10",
    )

    repo = _FakeRepo([(prompt, result)])
    matcher = CouponMatcher(["SAVE10"])
    notifier = _FakeNotifier()
    generator = WeeklyReportGenerator(repo, matcher, notifier)

    assert await generator.run_and_send(days=7) is True
    assert len(notifier.sent) == 1


@pytest.mark.asyncio
async def test_get_invalid_coupon_alerts_detects_untracked_patterns() -> None:
    prompt = AIOverviewPrompt(
        id=uuid4(),
        prompt_text="nordvpn coupon",
        primary_product="nordvpn",
        location="US",
    )
    result = AIOverviewResult(
        id=uuid4(),
        prompt_id=prompt.id,
        provider="google",
        scraped_date=date(2026, 1, 2),
        response_text="Try code: ABC123",
    )

    repo = _FakeRepo([(prompt, result)])
    matcher = CouponMatcher(["SAVE10"])
    notifier = _FakeNotifier()
    generator = WeeklyReportGenerator(repo, matcher, notifier)

    invalid = await generator.get_invalid_coupon_alerts(days=7)

    assert invalid
    assert all(m.coupon_code != "SAVE10" for m in invalid)
