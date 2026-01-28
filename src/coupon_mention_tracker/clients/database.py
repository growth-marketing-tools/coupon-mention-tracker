"""Database client using asyncpg connection pool."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import asyncpg

from coupon_mention_tracker.core.config import Settings
from coupon_mention_tracker.core.logger import get_logger


logger = get_logger(__name__)


class DatabaseClient:
    """Reusable database client that manages an asyncpg connection pool."""

    def __init__(self, settings: Settings) -> None:
        """Initialize the database client with application settings."""
        self._settings = settings
        self._pool: asyncpg.Pool | None = None

    @property
    def pool(self) -> asyncpg.Pool | None:
        """Return the active connection pool, if connected."""
        return self._pool

    async def connect(self) -> asyncpg.Pool:
        """Create the connection pool if needed and return it."""
        if self._pool is None:
            logger.info("[DATABASE] Creating asyncpg connection pool")
            self._pool = await asyncpg.create_pool(
                dsn=self._settings.database_url_str,
                min_size=1,
                max_size=5,
                command_timeout=60,
            )
        return self._pool

    async def disconnect(self) -> None:
        """Close the connection pool if it exists."""
        if self._pool is not None:
            logger.info("[DATABASE] Closing asyncpg connection pool")
            await self._pool.close()
            self._pool = None

    @asynccontextmanager
    async def acquire(self) -> AsyncGenerator[asyncpg.Connection, None]:
        """Acquire a database connection from the pool."""
        if self._pool is None:
            raise RuntimeError("Database not connected. Call connect() first.")
        async with self._pool.acquire() as conn:
            yield conn
