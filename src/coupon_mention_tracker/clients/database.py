"""Database connection pool with automatic JSON serialization."""

import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import ClassVar

import asyncpg

from coupon_mention_tracker.core.config import Settings
from coupon_mention_tracker.core.logger import get_logger


logger = get_logger(__name__)


class DatabasePool:
    """Manages the asyncpg connection pool."""

    _pool: ClassVar[asyncpg.Pool | None] = None

    @classmethod
    async def connect(cls, settings: Settings) -> None:
        """Initialize the connection pool."""
        if cls._pool is None:
            logger.info("[DATABASE] Creating asyncpg connection pool")
            cls._pool = await asyncpg.create_pool(
                dsn=settings.database_url_str,
                min_size=1,
                max_size=10,
                command_timeout=60,
                init=cls._init_connection,
            )

    @staticmethod
    async def _init_connection(conn: asyncpg.Connection) -> None:
        """Initialize connection with JSON codecs."""
        await conn.set_type_codec(
            "json",
            encoder=json.dumps,
            decoder=json.loads,
            schema="pg_catalog",
        )
        await conn.set_type_codec(
            "jsonb",
            encoder=json.dumps,
            decoder=json.loads,
            schema="pg_catalog",
        )

    @classmethod
    async def disconnect(cls) -> None:
        """Close the connection pool."""
        if cls._pool:
            logger.info("[DATABASE] Closing asyncpg connection pool")
            await cls._pool.close()
            cls._pool = None

    @classmethod
    @asynccontextmanager
    async def acquire(cls) -> AsyncGenerator[asyncpg.Connection, None]:
        """Acquire a connection from the pool."""
        if cls._pool is None:
            raise RuntimeError(
                "Database pool not initialized. Call connect() first."
            )
        async with cls._pool.acquire() as conn:
            yield conn
