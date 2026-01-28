"""Unit tests for SlackClient without sending network requests."""

from __future__ import annotations

from datetime import date
from http import HTTPStatus
from uuid import uuid4

import pytest
from slack_sdk.models.blocks import HeaderBlock, SectionBlock

from coupon_mention_tracker.clients.slack import MAX_DISPLAY_ITEMS, SlackClient
from coupon_mention_tracker.core.models import CouponMatch, WeeklyReportRow


@pytest.mark.asyncio
async def test_send_message_posts_payload(monkeypatch) -> None:
    calls = []

    class _Response:
        def __init__(self, status_code: int, body: str) -> None:
            self.status_code = status_code
            self.body = body

    class _MockClient:
        def __init__(self, url: str) -> None:
            self.url = url

        async def send(self, text, blocks=None):
            calls.append((self.url, text, blocks))
            return _Response(HTTPStatus.OK, "ok")

    monkeypatch.setattr(
        "coupon_mention_tracker.clients.slack.AsyncWebhookClient",
        _MockClient,
    )

    notifier = SlackClient(webhook_url="https://example.invalid")
    ok = await notifier.send_message("hello", blocks=[{"type": "divider"}])

    assert ok is True
    assert calls
    assert calls[0][0] == "https://example.invalid"
    assert calls[0][1] == "hello"
    assert calls[0][2] == [{"type": "divider"}]


def test_format_coupon_match_block_defaults_location_global() -> None:
    notifier = SlackClient(webhook_url="https://example.invalid")
    match = CouponMatch(
        keyword="k",
        location=None,
        product="p",
        scraped_date=date(2026, 1, 1),
        coupon_code="SAVE10",
        match_context="ctx",
        ai_overview_id=uuid4(),
    )

    block = notifier._format_coupon_match_block(match)
    assert isinstance(block, SectionBlock)
    assert block.text is not None
    assert "Global" in block.text.text


@pytest.mark.asyncio
async def test_send_coupon_alert_empty_is_noop() -> None:
    notifier = SlackClient(webhook_url="https://example.invalid")
    assert await notifier.send_coupon_alert([]) is True


@pytest.mark.asyncio
async def test_send_coupon_alert_truncates_and_adds_remaining(
    monkeypatch,
) -> None:
    captured = {}

    async def _send_message(text, blocks=None):
        captured["text"] = text
        captured["blocks"] = blocks
        return True

    notifier = SlackClient(webhook_url="https://example.invalid")
    monkeypatch.setattr(notifier, "send_message", _send_message)

    matches = [
        CouponMatch(
            keyword=f"k{i}",
            location=None,
            product="p",
            scraped_date=date(2026, 1, 1),
            coupon_code="SAVE10",
            match_context="ctx",
            ai_overview_id=uuid4(),
        )
        for i in range(MAX_DISPLAY_ITEMS + 1)
    ]

    ok = await notifier.send_coupon_alert(matches)

    assert ok is True
    assert "Found" in captured["text"]
    # Inspect block objects
    assert any(
        b.type == "context" and b.elements and "...and" in b.elements[0].text
        for b in captured["blocks"]
    )


def test_build_weekly_report_blocks_includes_summary_and_invalid() -> None:
    notifier = SlackClient(webhook_url="https://example.invalid")
    rows = [
        WeeklyReportRow(
            keyword="k1",
            location="US",
            product="p",
            has_ai_overview=True,
            coupon_detected="SAVE10",
            is_valid_coupon=True,
            first_seen=date(2026, 1, 1),
            last_seen=date(2026, 1, 1),
            mention_count=2,
        ),
        WeeklyReportRow(
            keyword="k2",
            location=None,
            product="p",
            has_ai_overview=True,
            coupon_detected="OLD10",
            is_valid_coupon=False,
            first_seen=date(2026, 1, 1),
            last_seen=date(2026, 1, 1),
            mention_count=1,
        ),
    ]

    blocks = notifier._build_weekly_report_blocks(
        report_rows=rows,
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 8),
    )

    # Extract text from SectionBlocks and HeaderBlocks
    lines = []
    for block in blocks:
        if isinstance(block, (SectionBlock, HeaderBlock)) and block.text:
            lines.append(block.text.text)
    text_blob = "\n".join(lines)
    assert "Untracked coupons detected" in text_blob
    assert "OLD10" in text_blob
