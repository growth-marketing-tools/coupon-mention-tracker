"""Main entry point for Coupon Mention Tracker Cloud Run Job."""

import asyncio
import logging
import sys
from datetime import date, timedelta

from coupon_mention_tracker.clients.slack import SlackNotifier
from coupon_mention_tracker.core.config import get_settings
from coupon_mention_tracker.repositories.ai_overview import (
    AIOverviewRepository,
)
from coupon_mention_tracker.services.coupon_matcher import CouponMatcher
from coupon_mention_tracker.services.report import WeeklyReportGenerator


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def run_weekly_report(days: int = 7, send_slack: bool = True) -> int:
    """Run the weekly coupon mention report.

    Args:
        days: Number of days to look back.
        send_slack: Whether to send the report to Slack.

    Returns:
        Exit code (0 for success, 1 for failure).
    """
    settings = get_settings()

    repository = AIOverviewRepository(settings.database_url_str)
    matcher = CouponMatcher(settings.coupons)
    notifier = SlackNotifier(
        webhook_url=settings.slack_webhook_url,
        default_channel=settings.slack_channel,
    )

    generator = WeeklyReportGenerator(repository, matcher, notifier)

    try:
        logger.info("Connecting to database...")
        await repository.connect()

        logger.info("Generating coupon mention report for last %d days", days)
        rows, matches = await generator.generate_report(days=days)

        end_date = date.today()
        start_date = end_date - timedelta(days=days)

        logger.info(
            "Report generated: %d keywords, %d with AI Overview, %d matches",
            len(rows),
            sum(1 for r in rows if r.has_ai_overview),
            len(matches),
        )

        with_coupons = [r for r in rows if r.coupon_detected]
        invalid_coupons = [
            r for r in with_coupons if r.is_valid_coupon is False
        ]

        if invalid_coupons:
            logger.warning(
                "Found %d invalid/outdated coupons in AI Overviews",
                len(invalid_coupons),
            )
            for row in invalid_coupons:
                logger.warning(
                    "  - %s in '%s' (%s)",
                    row.coupon_detected,
                    row.keyword,
                    row.location or "Global",
                )

        if send_slack:
            logger.info("Sending report to Slack...")
            success = await notifier.send_weekly_report(
                rows=rows,
                start_date=start_date,
                end_date=end_date,
            )
            if success:
                logger.info("Report sent to Slack successfully")
            else:
                logger.error("Failed to send report to Slack")
                return 1

        logger.info("Job completed successfully")
        return 0

    except Exception:
        logger.exception("Job failed with error")
        return 1

    finally:
        await repository.disconnect()


def main() -> None:
    """Main entry point for Cloud Run Job."""
    settings = get_settings()
    days = settings.report_lookback_days

    logger.info("Starting Coupon Mention Tracker job")
    logger.info("Tracking %d coupon codes", len(settings.coupons))

    exit_code = asyncio.run(run_weekly_report(days=days, send_slack=True))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
