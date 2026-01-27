"""Type-safe query builder using SQLAlchemy Core.

This module provides query builders that replace raw SQL strings
with type-safe SQLAlchemy Core constructs.
"""

from datetime import date
from typing import Any
from uuid import UUID

from sqlalchemy import and_, desc, select
from sqlalchemy.dialects.postgresql import insert

from coupon_mention_tracker.db.tables import (
    ai_overviews_prompts,
    ai_overviews_results,
    coupon_tracking_history,
)


def build_get_prompts_select(
    product: str | None = None,
    location: str | None = None,
    status: str = "active",
    tags: list[str] | None = None,
):
    """Build type-safe SELECT query for fetching prompts.

    Args:
        product: Filter by product (e.g., 'nordvpn', 'nordpass').
        location: Filter by location/country code.
        status: Filter by status (default: 'active').
        tags: Filter by tags. Prompts must have ALL specified tags.

    Returns:
        SQLAlchemy Select object that can be compiled to SQL.
    """
    query = select(
        ai_overviews_prompts.c.id,
        ai_overviews_prompts.c.prompt_text,
        ai_overviews_prompts.c.primary_product,
        ai_overviews_prompts.c.location,
        ai_overviews_prompts.c.status,
        ai_overviews_prompts.c.tags,
        ai_overviews_prompts.c.created_at,
    )

    conditions = []

    if status:
        conditions.append(ai_overviews_prompts.c.status == status)

    if product:
        conditions.append(ai_overviews_prompts.c.primary_product == product)

    if location:
        conditions.append(ai_overviews_prompts.c.location == location)

    if tags:
        # PostgreSQL array contains operator (@>)
        conditions.append(ai_overviews_prompts.c.tags.contains(tags))

    if conditions:
        query = query.where(and_(*conditions))

    query = query.order_by(desc(ai_overviews_prompts.c.created_at))

    return query


def build_get_results_for_period_select(
    start_date: date,
    end_date: date,
    provider: str = "google_ai_overview",
    tags: list[str] | None = None,
):
    """Build type-safe SELECT query for fetching results with prompts.

    Args:
        start_date: Start of the period (inclusive).
        end_date: End of the period (inclusive).
        provider: AI provider to filter by.
        tags: Filter by tags. Prompts must have ALL specified tags.

    Returns:
        SQLAlchemy Select object that can be compiled to SQL.
    """
    p = ai_overviews_prompts.alias("prompt")
    r = ai_overviews_results.alias("result")

    query = select(
        p.c.id.label("prompt_id"),
        p.c.prompt_text,
        p.c.primary_product,
        p.c.location,
        p.c.status,
        p.c.tags,
        p.c.created_at.label("prompt_created_at"),
        r.c.id.label("result_id"),
        r.c.provider,
        r.c.scraped_date,
        r.c.scraped_at,
        r.c.response_text,
        r.c.sources,
        r.c.ahrefs_volume,
        r.c.sentiment_label,
    ).select_from(r.join(p, r.c.prompt_id == p.c.id))

    conditions = [
        r.c.scraped_date.between(start_date, end_date),
        r.c.provider == provider,
        r.c.response_text.isnot(None),
    ]

    if tags:
        conditions.append(p.c.tags.contains(tags))

    query = query.where(and_(*conditions))

    query = query.order_by(
        desc(r.c.scraped_date),
        p.c.prompt_text,
    )

    return query


def build_get_sources_for_results_select(result_ids: list[UUID | str]):
    """Build type-safe SELECT for fetching sources with HTML content.

    This is a complex query that:
    1. Expands JSONB array of sources in results
    2. Joins with sources table on URL
    3. Filters for sources with HTML content

    Note: Due to complexity of LATERAL joins with JSONB in SQLAlchemy,
    we use text() for the core query logic but still parameterize properly.

    Args:
        result_ids: List of result UUIDs to fetch sources for.

    Returns:
        SQLAlchemy Select object that can be compiled to SQL.
    """
    from sqlalchemy import text

    query = text(
        """
        SELECT DISTINCT ON (source_item->>'url', r.id)
            s.id,
            source_item->>'url' as source_url,
            s.source_domain,
            s.source_html_content,
            s.page_title,
            s.scraped_at,
            s.scrape_status,
            r.id as result_id
        FROM marketing_hub.ai_overviews_results r
        CROSS JOIN LATERAL jsonb_array_elements(
            CASE
                WHEN jsonb_typeof(r.sources) = 'array' THEN r.sources
                ELSE '[]'::jsonb
            END
        ) AS source_item
        JOIN marketing_hub.ai_overviews_sources s
            ON s.source_url = SPLIT_PART(source_item->>'url', '#', 1)
        WHERE r.id = ANY(:result_ids)
          AND s.source_html_content IS NOT NULL
          AND s.scrape_status = 'success'
        ORDER BY source_item->>'url', r.id, s.scraped_at DESC
        """
    ).bindparams(result_ids=result_ids)

    return query


def build_upsert_tracking_history(
    keyword: str,
    scraped_date: date,
    has_ai_overview: bool,
    location: str | None = None,
    primary_product: str | None = None,
    ai_overview_result_id: UUID | None = None,
    tracked_coupon_present: bool = False,
    detected_coupon_code: str | None = None,
    is_valid_coupon: bool | None = None,
    match_context: str | None = None,
    source_mention_count: int = 0,
    source_urls_with_mentions: list[str] | None = None,
    source_mention_unavailable: bool = False,
):
    """Build type-safe INSERT with ON CONFLICT for tracking history.

    Args:
        keyword: The search keyword being tracked.
        scraped_date: Date the AI Overview was scraped.
        has_ai_overview: Whether an AI Overview was present.
        location: Geographic location.
        primary_product: Product category.
        ai_overview_result_id: Reference to the AI overview result.
        tracked_coupon_present: Whether a tracked coupon was found.
        detected_coupon_code: The coupon code detected.
        is_valid_coupon: Whether the coupon is in the active list.
        match_context: Text snippet showing the coupon.
        source_mention_count: Number of sources with mentions.
        source_urls_with_mentions: URLs where coupon was found.
        source_mention_unavailable: True if sources unavailable.

    Returns:
        SQLAlchemy Insert object with ON CONFLICT clause.
    """
    stmt = insert(coupon_tracking_history).values(
        keyword=keyword,
        location=location,
        primary_product=primary_product,
        has_ai_overview=has_ai_overview,
        ai_overview_result_id=ai_overview_result_id,
        tracked_coupon_present=tracked_coupon_present,
        detected_coupon_code=detected_coupon_code,
        is_valid_coupon=is_valid_coupon,
        match_context=match_context,
        scraped_date=scraped_date,
        source_mention_count=source_mention_count,
        source_urls_with_mentions=source_urls_with_mentions or [],
        source_mention_unavailable=source_mention_unavailable,
    )

    stmt = stmt.on_conflict_do_update(
        index_elements=["keyword", "location", "scraped_date"],
        set_={
            "has_ai_overview": stmt.excluded.has_ai_overview,
            "ai_overview_result_id": stmt.excluded.ai_overview_result_id,
            "tracked_coupon_present": stmt.excluded.tracked_coupon_present,
            "detected_coupon_code": stmt.excluded.detected_coupon_code,
            "is_valid_coupon": stmt.excluded.is_valid_coupon,
            "match_context": stmt.excluded.match_context,
            "source_mention_count": stmt.excluded.source_mention_count,
            "source_urls_with_mentions": (
                stmt.excluded.source_urls_with_mentions
            ),
            "source_mention_unavailable": (
                stmt.excluded.source_mention_unavailable
            ),
        },
    )

    return stmt


def compile_query(
    query, _dialect_name: str = "postgresql"
) -> tuple[str, list[Any]]:
    """Compile SQLAlchemy query to SQL string with positional parameters.

    Args:
        query: SQLAlchemy Select, Insert, or TextClause object.
        dialect_name: SQL dialect (default: 'postgresql').

    Returns:
        Tuple of (sql_string, parameters) ready for asyncpg execution.
    """
    import re

    from sqlalchemy import TextClause
    from sqlalchemy.dialects import postgresql

    if isinstance(query, TextClause):
        compiled = query.compile(dialect=postgresql.dialect())
        sql_string = str(compiled)
        params = compiled.params
    else:
        compiled = query.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": False},
        )
        sql_string = str(compiled)
        params = compiled.params

    # Convert %(name)s to $1, $2, etc. for asyncpg
    matches = re.findall(r"%\(([^)]+)\)s", sql_string)
    param_list = []
    for i, name in enumerate(matches, 1):
        sql_string = sql_string.replace(f"%({name})s", f"${i}", 1)
        param_list.append(params[name])

    return sql_string, param_list
