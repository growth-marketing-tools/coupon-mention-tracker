"""Repository for writing coupon tracking data to Looker schema."""

from datetime import date
from uuid import UUID

from coupon_mention_tracker.clients.database import DatabasePool
from coupon_mention_tracker.core.logger import get_logger
from coupon_mention_tracker.repositories import sql_queries


logger = get_logger(__name__)


class LookerRepository:
    """Repository for writing to the looker schema for dashboard reporting."""

    def __init__(self) -> None:
        """Initialize repository."""
        pass

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
        params = (
            keyword,
            location,
            primary_product,
            has_ai_overview,
            ai_overview_result_id,
            tracked_coupon_present,
            detected_coupon_code,
            is_valid_coupon,
            match_context,
            scraped_date,
            source_mention_count,
            source_urls_with_mentions or [],
            source_mention_unavailable,
        )

        async with DatabasePool.acquire() as conn:
            await conn.execute(sql_queries.UPSERT_TRACKING_HISTORY, *params)

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

        params = [
            (
                r["keyword"],
                r.get("location"),
                r.get("primary_product"),
                r["has_ai_overview"],
                r.get("ai_overview_result_id"),
                r.get("tracked_coupon_present", False),
                r.get("detected_coupon_code"),
                r.get("is_valid_coupon"),
                r.get("match_context"),
                r["scraped_date"],
                r.get("source_mention_count", 0),
                r.get("source_urls_with_mentions") or [],
                r.get("source_mention_unavailable", False),
            )
            for r in records
        ]

        async with DatabasePool.acquire() as conn:
            await conn.executemany(sql_queries.UPSERT_TRACKING_HISTORY, params)

        logger.info(
            "[LOOKER] Saved %d tracking records to looker schema", len(records)
        )
        return len(records)
