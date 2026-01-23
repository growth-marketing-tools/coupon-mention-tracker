"""Repository for writing coupon tracking data to Looker schema."""

from datetime import date
from uuid import UUID

import asyncpg

from coupon_mention_tracker.clients import CloudSQLPool
from coupon_mention_tracker.core.logger import get_logger
from coupon_mention_tracker.db.sql_query_builder import (
    build_upsert_tracking_history,
    compile_query,
)


logger = get_logger(__name__)


class LookerRepository:
    """Repository for writing to the looker schema for dashboard reporting."""

    def __init__(self, pool: asyncpg.Pool | CloudSQLPool) -> None:
        """Initialize repository with an existing connection pool.

        Args:
            pool: asyncpg connection pool or CloudSQLPool.
        """
        self._pool = pool

    async def save_tracking_record(
        self,
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
    ) -> None:
        """Save a single tracking record to the looker schema.

        Uses upsert to handle duplicate entries.

        Args:
            keyword: The search keyword being tracked.
            scraped_date: Date the AI Overview was scraped.
            has_ai_overview: Whether an AI Overview was present.
            location: Geographic location (e.g., US, UK).
            primary_product: Product category.
            ai_overview_result_id: Reference to the AI overview result.
            tracked_coupon_present: Whether a tracked coupon was found.
            detected_coupon_code: The coupon code detected (if any).
            is_valid_coupon: Whether the coupon is in the active list.
            match_context: Text snippet showing the coupon in context.
            source_mention_count: Number of sources where coupon was found.
            source_urls_with_mentions: URLs where coupon was found.
            source_mention_unavailable: True if we could not check sources.
        """
        insert_stmt = build_upsert_tracking_history(
            keyword=keyword,
            scraped_date=scraped_date,
            has_ai_overview=has_ai_overview,
            location=location,
            primary_product=primary_product,
            ai_overview_result_id=ai_overview_result_id,
            tracked_coupon_present=tracked_coupon_present,
            detected_coupon_code=detected_coupon_code,
            is_valid_coupon=is_valid_coupon,
            match_context=match_context,
            source_mention_count=source_mention_count,
            source_urls_with_mentions=source_urls_with_mentions,
            source_mention_unavailable=source_mention_unavailable,
        )
        query, params = compile_query(insert_stmt)

        async with self._pool.acquire() as conn:
            await conn.execute(query, *params)

    async def save_tracking_batch(
        self,
        records: list[dict],
    ) -> int:
        """Save multiple tracking records in a batch.

        Args:
            records: List of dicts with tracking data. Each dict should have:
                - keyword (str, required)
                - scraped_date (date, required)
                - has_ai_overview (bool, required)
                - location (str | None)
                - primary_product (str | None)
                - ai_overview_result_id (UUID | None)
                - tracked_coupon_present (bool)
                - detected_coupon_code (str | None)
                - is_valid_coupon (bool | None)
                - match_context (str | None)
                - source_mention_count (int)
                - source_urls_with_mentions (list[str] | None)
                - source_mention_unavailable (bool)

        Returns:
            Number of records saved.
        """
        if not records:
            return 0

        # Build a single upsert query (we'll use it for batch operations)
        # For batch, we need to execute each separately since SQLAlchemy
        # doesn't support batched upserts easily
        async with self._pool.acquire() as conn:
            for record in records:
                insert_stmt = build_upsert_tracking_history(
                    keyword=record["keyword"],
                    scraped_date=record["scraped_date"],
                    has_ai_overview=record["has_ai_overview"],
                    location=record.get("location"),
                    primary_product=record.get("primary_product"),
                    ai_overview_result_id=record.get("ai_overview_result_id"),
                    tracked_coupon_present=record.get(
                        "tracked_coupon_present", False
                    ),
                    detected_coupon_code=record.get("detected_coupon_code"),
                    is_valid_coupon=record.get("is_valid_coupon"),
                    match_context=record.get("match_context"),
                    source_mention_count=record.get("source_mention_count", 0),
                    source_urls_with_mentions=record.get(
                        "source_urls_with_mentions"
                    ),
                    source_mention_unavailable=record.get(
                        "source_mention_unavailable", False
                    ),
                )
                query, params = compile_query(insert_stmt)
                await conn.execute(query, *params)

        logger.info(
            "[LOOKER] Saved %d tracking records to looker schema", len(records)
        )
        return len(records)
