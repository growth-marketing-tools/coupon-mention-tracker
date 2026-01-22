"""Repository for querying AI Overview data from PostgreSQL."""

import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import date, timedelta

import asyncpg

from coupon_mention_tracker.clients import CloudSQLPool, create_db_pool
from coupon_mention_tracker.core.config import Settings
from coupon_mention_tracker.core.models import (
    AIOverviewPrompt,
    AIOverviewResult,
)
from coupon_mention_tracker.repositories.sql_queries import (
    GET_RESULTS_FOR_PERIOD_QUERY,
    build_get_prompts_query,
)


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


class AIOverviewRepository:
    """Repository for interacting with the AI Overviews database."""

    def __init__(self, settings: Settings) -> None:
        """Initialize repository.

        Args:
            settings: Application settings.
        """
        self._settings = settings
        self._pool: asyncpg.Pool | CloudSQLPool | None = None

    async def connect(self) -> None:
        """Establish database connection pool."""
        self._pool = await create_db_pool(self._settings)

    async def disconnect(self) -> None:
        """Close database connection pool."""
        if self._pool:
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
    ) -> list[AIOverviewPrompt]:
        """Fetch AI Overview prompts/keywords.

        Args:
            product: Filter by product (e.g., 'nordvpn', 'nordpass').
            location: Filter by location/country code.
            status: Filter by status (default: 'active').

        Returns:
            List of AI Overview prompts.
        """
        query, params = build_get_prompts_query(
            product=product,
            location=location,
            status=status,
        )

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
        end_date: date | None = None,
        provider: str = "google_ai_overview",
    ) -> list[tuple[AIOverviewPrompt, AIOverviewResult]]:
        """Fetch AI Overview results with their prompts for a date range.

        Args:
            start_date: Start of the period (inclusive).
            end_date: End of the period (inclusive). Defaults to today.
            provider: AI provider to filter by (default: 'google').

        Returns:
            List of tuples containing (prompt, result) pairs.
        """
        if end_date is None:
            end_date = date.today()

        async with self.acquire() as conn:
            rows = await conn.fetch(
                GET_RESULTS_FOR_PERIOD_QUERY,
                start_date,
                end_date,
                provider,
            )

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
                sources=_parse_sources(row["sources"]),
                ahrefs_volume=row["ahrefs_volume"],
                sentiment_label=row["sentiment_label"],
            )
            results.append((prompt, result))

        return results

    async def get_results_last_n_days(
        self,
        days: int = 7,
        provider: str = "google_ai_overview",
    ) -> list[tuple[AIOverviewPrompt, AIOverviewResult]]:
        """Fetch AI Overview results from the last N days.

        Args:
            days: Number of days to look back (default: 7).
            provider: AI provider to filter by (default: 'google').

        Returns:
            List of tuples containing (prompt, result) pairs.
        """
        start_date = date.today() - timedelta(days=days)
        return await self.get_results_for_period(
            start_date=start_date,
            end_date=date.today(),
            provider=provider,
        )
