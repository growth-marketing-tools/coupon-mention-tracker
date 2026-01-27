"""Database client using asyncpg connection pool."""

import asyncpg

from coupon_mention_tracker.core.config import Settings
from coupon_mention_tracker.core.logger import get_logger


logger = get_logger(__name__)


async def create_db_pool(settings: Settings) -> asyncpg.Pool:
    """Create a database connection pool.

    Args:
        settings: Application settings containing database URL.

    Returns:
        Configured asyncpg connection pool.
    """
    logger.info("[DATABASE] Creating asyncpg connection pool")
    return await asyncpg.create_pool(
        dsn=settings.database_url_str,
        min_size=1,
        max_size=5,
        command_timeout=60,
    )
