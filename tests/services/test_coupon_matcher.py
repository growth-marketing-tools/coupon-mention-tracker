"""Tests for coupon matching."""

from __future__ import annotations

from datetime import date
from uuid import uuid4

from coupon_mention_tracker.core.models import (
    AIOverviewPrompt,
    AIOverviewResult,
)
from coupon_mention_tracker.services.coupon_matcher import CouponMatcher


def test_find_matches_word_boundaries_and_case_insensitive() -> None:
    matcher = CouponMatcher(["save10"])
    text = "Use SAVE10 today. Later, save10 again! Not SAVE100."

    matches = matcher.find_matches(text)

    assert [m.coupon_code for m in matches] == ["SAVE10", "SAVE10"]
    assert all("SAVE10" in m.context.upper() for m in matches)


def test_find_matches_does_not_match_substrings() -> None:
    matcher = CouponMatcher(["ABC"])

    assert matcher.find_matches("Use ABCD") == []


def test_find_any_coupon_pattern_finds_untracked_codes() -> None:
    matcher = CouponMatcher([])
    found = matcher.find_any_coupon_pattern("Try code: abc123 and NORDVPNDEAL")

    upper = set(found)
    assert "ABC123" in upper
    assert "CODE: ABC123" in upper
    assert "NORDVPNDEAL" in upper


def test_analyze_result_builds_coupon_matches() -> None:
    matcher = CouponMatcher(["SAVE10"], context_chars=10)
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
        scraped_date=date(2026, 1, 1),
        response_text="Use SAVE10 now",
    )

    matches = matcher.analyze_result(prompt, result)

    assert len(matches) == 1
    assert matches[0].coupon_code == "SAVE10"
    assert matches[0].keyword == "nordvpn coupon"
    assert matches[0].location == "US"
    assert matches[0].product == "nordvpn"


def test_analyze_result_returns_empty_for_missing_text() -> None:
    matcher = CouponMatcher(["SAVE10"])
    prompt = AIOverviewPrompt(
        id=uuid4(),
        prompt_text="x",
        primary_product="nordvpn",
    )
    result = AIOverviewResult(
        id=uuid4(),
        prompt_id=prompt.id,
        provider="google",
        scraped_date=date(2026, 1, 1),
        response_text=None,
    )

    assert matcher.analyze_result(prompt, result) == []
