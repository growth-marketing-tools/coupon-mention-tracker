"""Tests for SQL query builders."""

from coupon_mention_tracker.repositories.sql_queries import (
    build_get_prompts_query,
)


def test_build_get_prompts_query_includes_all_filters_in_order() -> None:
    query, params = build_get_prompts_query(
        product="nordvpn",
        location="US",
        status="active",
    )

    assert "status = $1" in query
    assert "primary_product = $2" in query
    assert "location = $3" in query
    assert params == ["active", "nordvpn", "US"]


def test_build_get_prompts_query_skips_empty_status() -> None:
    query, params = build_get_prompts_query(
        product="nordvpn",
        location="US",
        status="",
    )

    assert "status = $" not in query
    assert "primary_product = $1" in query
    assert "location = $2" in query
    assert params == ["nordvpn", "US"]
