"""Database client with Cloud SQL and standard asyncpg support."""

import asyncio
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING
from urllib.parse import unquote, urlparse

import asyncpg
from google.cloud.sql.connector import Connector

from coupon_mention_tracker.core.config import Settings
from coupon_mention_tracker.core.logger import get_logger


if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

logger = get_logger(__name__)


class CloudSQLPool:
    """A wrapper that mimics asyncpg.Pool but uses Cloud SQL Connector.

    This acts as a pool of size 1, ensuring thread/task safety via a Lock.
    Ideal for Cloud Run Jobs or scripts where high concurrency is not required.
    """

    def __init__(self, settings: Settings) -> None:
        """Initialize with settings."""
        self._settings = settings
        self._connector = Connector()
        self._conn: asyncpg.Connection | None = None
        self._lock = asyncio.Lock()
        # Cloud SQL Connector requires individual credentials (no DSN support)
        parsed = urlparse(settings.database_url_str)
        self._user = parsed.username or ""
        self._password = unquote(parsed.password or "")
        self._db = parsed.path.lstrip("/") if parsed.path else ""

    async def _get_conn(self) -> asyncpg.Connection:
        """Get or create a connection."""
        if self._conn is None or self._conn.is_closed():
            logger.info(
                "[DATABASE] Establishing new Cloud SQL connection to %s...",
                self._settings.cloud_sql_instance_connection_name,
            )
            self._conn = await self._connector.connect_async(
                str(self._settings.cloud_sql_instance_connection_name),
                "asyncpg",
                user=self._user,
                password=self._password,
                db=self._db,
            )
        return self._conn

    @asynccontextmanager
    async def acquire(self) -> "AsyncGenerator[asyncpg.Connection, None]":
        """Acquire a connection from the 'pool'."""
        async with self._lock:
            conn = await self._get_conn()
            yield conn

    async def close(self) -> None:
        """Close the connection and the connector."""
        if self._conn and not self._conn.is_closed():
            await self._conn.close()
        connector_close_result = self._connector.close()
        if connector_close_result is not None:
            await connector_close_result


async def create_db_pool(settings: Settings) -> asyncpg.Pool | CloudSQLPool:
    """Create a database connection pool based on settings.

    If cloud_sql_instance_connection_name is set, returns a CloudSQLPool.
    Otherwise, returns a standard asyncpg.Pool.
    """
    if settings.cloud_sql_instance_connection_name:
        logger.info(
            "[DATABASE] Using Cloud SQL Connector for database connection"
        )
        return CloudSQLPool(settings)

    logger.info("[DATABASE] Using standard asyncpg pool")
    return await asyncpg.create_pool(
        dsn=settings.database_url_str,
        min_size=1,
        max_size=10,
        ssl=False,  # Proxy handles encryption
    )
