"""Repository for querying AI Overview data from PostgreSQL."""

import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import date, timedelta
from uuid import UUID

import asyncpg

from coupon_mention_tracker.clients import CloudSQLPool, create_db_pool
from coupon_mention_tracker.core.config import Settings
from coupon_mention_tracker.core.logger import get_logger
from coupon_mention_tracker.core.models import (
    AIOverviewPrompt,
    AIOverviewResult,
)
from coupon_mention_tracker.db.sql_query_builder import (
    build_get_prompts_select,
    build_get_results_for_period_select,
    build_get_sources_for_results_select,
    compile_query,
)


logger = get_logger(__name__)


class AIOverviewRepository:
    """Repository for interacting with the AI Overviews database."""

    @staticmethod
    def _parse_sources(sources: str | list | None) -> list[dict] | None:
        """Parse sources from database JSON string to list of dicts."""
        if sources is None:
            return None
        if isinstance(sources, list):
            return sources
        if isinstance(sources, str):
            try:
                parsed = json.loads(sources)
                return parsed if isinstance(parsed, list) else None
            except json.JSONDecodeError:
                return None
        return None

    def __init__(self, settings: Settings) -> None:
        """Initialize repository.

        Args:
            settings: Application settings.
        """
        self._settings = settings
        self._pool: asyncpg.Pool | CloudSQLPool | None = None

    async def connect(self) -> None:
        """Establish database connection pool."""
        logger.info(
            "[DATABASE] Connecting to database for AIOverviewRepository..."
        )
        self._pool = await create_db_pool(self._settings)

    async def disconnect(self) -> None:
        """Close database connection pool."""
        if self._pool:
            logger.info(
                "[DATABASE] Disconnecting from database for "
                "AIOverviewRepository..."
            )
            await self._pool.close()
            self._pool = None

    @asynccontextmanager
    async def acquire(self) -> AsyncGenerator[asyncpg.Connection, None]:
        """Acquire a database connection from the pool."""
        if not self._pool:
            raise RuntimeError("Database not connected. Call connect() first.")
        async with self._pool.acquire() as conn:
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
        select_stmt = build_get_prompts_select(
            product=product,
            location=location,
            status=status,
            tags=tags,
        )
        query, params = compile_query(select_stmt)

        async with self.acquire() as conn:
            rows = await conn.fetch(query, *params)

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
        select_stmt = build_get_results_for_period_select(
            start_date=start_date,
            end_date=end_date,
            provider=provider,
            tags=tags,
        )
        query, params = compile_query(select_stmt)

        async with self.acquire() as conn:
            rows = await conn.fetch(query, *params)

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
                sources=self._parse_sources(row["sources"]),
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

        select_stmt = build_get_sources_for_results_select(result_ids)
        query, params = compile_query(select_stmt)

        async with self.acquire() as conn:
            rows = await conn.fetch(query, *params)

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
