"""Repository for querying AI Overview data from PostgreSQL."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import date, timedelta
from uuid import UUID

import asyncpg

from coupon_mention_tracker.clients.database import DatabasePool
from coupon_mention_tracker.core.config import Settings
from coupon_mention_tracker.core.logger import get_logger
from coupon_mention_tracker.core.models import (
    AIOverviewPrompt,
    AIOverviewResult,
)
from coupon_mention_tracker.repositories import sql_queries


logger = get_logger(__name__)


class AIOverviewRepository:
    """Repository for interacting with the AI Overviews database."""

    def __init__(self, settings: Settings) -> None:
        """Initialize repository.

        Args:
            settings: Application settings.
        """
        self._settings = settings

    @property
    def pool(self) -> asyncpg.Pool | None:
        """Return the active connection pool, if connected."""
        return DatabasePool._pool

    async def connect(self) -> None:
        """Establish database connection pool."""
        logger.info(
            "[DATABASE] Connecting to database for AIOverviewRepository..."
        )
        await DatabasePool.connect(self._settings)

    async def disconnect(self) -> None:
        """Close database connection pool."""
        if DatabasePool._pool:
            logger.info(
                "[DATABASE] Disconnecting from database for "
                "AIOverviewRepository..."
            )
            await DatabasePool.disconnect()

    @asynccontextmanager
    async def acquire(self) -> AsyncGenerator[asyncpg.Connection, None]:
        """Acquire a database connection from the pool."""
        async with DatabasePool.acquire() as conn:
            yield conn

    async def get_prompts(
        self,
        product: str | None = None,
        location: str | None = None,
        status: str = "active",
        tags: list[str] | None = None,
    ) -> list[AIOverviewPrompt]:
        """Fetch AI Overview prompts/keywords.

        Args:
            product: Filter by product (e.g., 'nordvpn', 'nordpass').
            location: Filter by location/country code.
            status: Filter by status (default: 'active').
            tags: Filter by tags. Prompts must have ALL specified tags.

        Returns:
            List of AI Overview prompts.
        """
        # 1. Start with base query
        query_parts = [sql_queries.GET_PROMPTS_BASE.strip()]
        params = [status]

        # 2. Append dynamic conditions
        if product:
            params.append(product)
            query_parts.append(f"AND primary_product = ${len(params)}")

        if location:
            params.append(location)
            query_parts.append(f"AND location = ${len(params)}")

        if tags:
            params.append(tags)
            # Postgres array containment operator
            query_parts.append(f"AND tags @> ${len(params)}")

        query_parts.append("ORDER BY created_at DESC")

        # 3. Execute
        final_query = "\n".join(query_parts)

        async with DatabasePool.acquire() as conn:
            rows = await conn.fetch(final_query, *params)

        return [
            AIOverviewPrompt(
                id=row["id"],
                prompt_text=row["prompt_text"],
                primary_product=row["primary_product"],
                location=row["location"],
                status=row["status"],
                tags=row["tags"],
                created_at=row["created_at"],
            )
            for row in rows
        ]

    async def get_results_for_period(
        self,
        start_date: date,
        end_date: date,
        provider: str = "google_ai_overview",
        tags: list[str] | None = None,
    ) -> list[tuple[AIOverviewPrompt, AIOverviewResult]]:
        """Fetch AI Overview results with their prompts for a date range.

        Args:
            start_date: Start of the period (inclusive).
            end_date: End of the period (inclusive).
            provider: AI provider to filter by (default: 'google').
            tags: Filter by tags. Prompts must have ALL specified tags.

        Returns:
            List of tuples containing (prompt, result) pairs.
        """
        # Base query has 3 params: $1, $2, $3
        query_parts = [sql_queries.GET_RESULTS_FOR_PERIOD.strip()]
        params = [start_date, end_date, provider]

        if tags:
            params.append(tags)
            query_parts.append(f"AND p.tags @> ${len(params)}")

        query_parts.append("ORDER BY r.scraped_date DESC, p.prompt_text")

        final_query = "\n".join(query_parts)

        async with DatabasePool.acquire() as conn:
            rows = await conn.fetch(final_query, *params)

        results = []
        for row in rows:
            prompt = AIOverviewPrompt(
                id=row["prompt_id"],
                prompt_text=row["prompt_text"],
                primary_product=row["primary_product"],
                location=row["location"],
                status=row["status"],
                tags=row["tags"],
                created_at=row["prompt_created_at"],
            )
            result = AIOverviewResult(
                id=row["result_id"],
                prompt_id=row["prompt_id"],
                provider=row["provider"],
                scraped_date=row["scraped_date"],
                scraped_at=row["scraped_at"],
                response_text=row["response_text"],
                # JSON/JSONB fields are automatically parsed by DatabasePool
                sources=row["sources"],
                ahrefs_volume=row["ahrefs_volume"],
                sentiment_label=row["sentiment_label"],
            )
            results.append((prompt, result))

        return results

    async def get_results_last_n_days(
        self,
        days: int = 7,
        provider: str = "google_ai_overview",
        tags: list[str] | None = None,
    ) -> list[tuple[AIOverviewPrompt, AIOverviewResult]]:
        """Fetch AI Overview results from the last N days.

        Args:
            days: Number of days to look back (default: 7).
            provider: AI provider to filter by (default: 'google').
            tags: Filter by tags. Prompts must have ALL specified tags.

        Returns:
            List of tuples containing (prompt, result) pairs.
        """
        start_date = date.today() - timedelta(days=days)
        return await self.get_results_for_period(
            start_date=start_date,
            end_date=date.today(),
            provider=provider,
            tags=tags,
        )

    async def get_sources_with_html(
        self, result_ids: list[UUID | str]
    ) -> dict[str, list[dict]]:
        """Fetch source HTML content for given result IDs.

        Args:
            result_ids: List of result UUIDs to fetch sources for.

        Returns:
            Mapping of result_id to source list with HTML.
        """
        if not result_ids:
            return {}

        async with DatabasePool.acquire() as conn:
            rows = await conn.fetch(
                sql_queries.GET_SOURCES_FOR_RESULTS, result_ids
            )

        sources_by_result: dict[str, list[dict]] = {}
        for row in rows:
            result_id = str(row["result_id"])
            if result_id not in sources_by_result:
                sources_by_result[result_id] = []

            sources_by_result[result_id].append(
                {
                    "id": row["id"],
                    "source_url": row["source_url"],
                    "source_domain": row["source_domain"],
                    "source_html_content": row["source_html_content"],
                    "page_title": row["page_title"],
                    "scraped_at": row["scraped_at"],
                    "scrape_status": row["scrape_status"],
                }
            )

        return sources_by_result
