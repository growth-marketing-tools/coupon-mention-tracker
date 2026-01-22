"""Main entry point for Coupon Mention Tracker Cloud Run Job."""

import asyncio
import sys
from datetime import date, timedelta

from coupon_mention_tracker.clients.google_sheets_client import (
    GoogleSheetsClient,
)
from coupon_mention_tracker.clients.slack_client import SlackNotifier
from coupon_mention_tracker.core.config import Settings, get_settings
from coupon_mention_tracker.core.logger import get_logger, setup_logging
from coupon_mention_tracker.core.models import (
    AIOverviewPrompt,
    AIOverviewResult,
    CouponMatch,
)
from coupon_mention_tracker.repositories.ai_overview_repository import (
    AIOverviewRepository,
)
from coupon_mention_tracker.repositories.looker_repository import LookerRepository
from coupon_mention_tracker.services.coupon_matcher import CouponMatcher
from coupon_mention_tracker.services.report import WeeklyReportGenerator


logger = get_logger(__name__)


def build_tracking_records(
    results: list[tuple[AIOverviewPrompt, AIOverviewResult]],
    matches: list[CouponMatch],
    matcher: CouponMatcher,
) -> list[dict]:
    """Build daily tracking records for Looker from raw results and matches.

    Args:
        results: List of (prompt, result) pairs from the database.
        matches: List of coupon matches found during analysis.
        matcher: Coupon matcher for validation.

    Returns:
        List of tracking records ready for Looker insertion.
    """
    # Index matches by (keyword, location, scraped_date) for quick lookup
    match_index: dict[tuple, CouponMatch] = {}
    for match in matches:
        key = (match.keyword, match.location, match.scraped_date)
        # Keep first match per keyword/location/date (or could aggregate)
        if key not in match_index:
            match_index[key] = match

    records = []
    for prompt, result in results:
        key = (prompt.prompt_text, prompt.location, result.scraped_date)
        match = match_index.get(key)

        records.append(
            {
                "keyword": prompt.prompt_text,
                "location": prompt.location,
                "primary_product": prompt.primary_product,
                "has_ai_overview": True,  # We have a result, so AI Overview exists
                "ai_overview_result_id": result.id,
                "tracked_coupon_present": match is not None,
                "detected_coupon_code": match.coupon_code if match else None,
                "is_valid_coupon": (
                    matcher.is_valid_coupon(match.coupon_code) if match else None
                ),
                "match_context": match.match_context if match else None,
                "scraped_date": result.scraped_date,
            }
        )

    return records


def fetch_coupons_from_google_sheets(settings: Settings) -> list[str]:
    """Fetch coupon codes from Google Sheets."""
    client = GoogleSheetsClient(
        spreadsheet_id=settings.google_sheets_spreadsheet_id,
        credentials_json=settings.google_workspace_credentials,
    )
    return client.get_coupons(
        gid=settings.google_sheets_coupon_gid,
        column_name=settings.google_sheets_coupon_column,
    )


async def run_weekly_report(days: int = 7, send_slack: bool = True) -> int:
    """Run the weekly coupon mention report.

    Args:
        days: Number of days to look back.
        send_slack: Whether to send the report to Slack.

    Returns:
        Exit code (0 for success, 1 for failure).
    """
    settings = get_settings()
    repository: AIOverviewRepository | None = None

    try:
        coupons = fetch_coupons_from_google_sheets(settings)
        logger.info("Tracking %d coupon codes", len(coupons))
        if not coupons:
            logger.warning("No coupon codes found; matcher will never match")

        repository = AIOverviewRepository(settings)
        matcher = CouponMatcher(coupons)
        notifier = SlackNotifier(
            webhook_url=settings.slack_webhook_url,
            default_channel=settings.slack_channel,
        )
        generator = WeeklyReportGenerator(repository, matcher, notifier)

        logger.info("Connecting to database...")
        await repository.connect()

        logger.info("Generating coupon mention report for last %d days", days)
        rows, matches = await generator.generate_report(days=days)

        # Fetch raw results for Looker tracking
        raw_results = await repository.get_results_last_n_days(days=days)

        end_date = date.today()
        start_date = end_date - timedelta(days=days)

        unique_keywords = {(r.keyword, r.location) for r in rows}
        keywords_with_overview = {
            (r.keyword, r.location) for r in rows if r.has_ai_overview
        }
        logger.info(
            "Report generated: %d keywords, %d with AI Overview, %d matches",
            len(unique_keywords),
            len(keywords_with_overview),
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

        # Save tracking data to Looker schema for dashboard
        logger.info("Saving tracking data to Looker schema...")
        tracking_records = build_tracking_records(raw_results, matches, matcher)
        if tracking_records and repository._pool:
            looker_repo = LookerRepository(repository._pool)
            saved_count = await looker_repo.save_tracking_batch(tracking_records)
            logger.info("Saved %d tracking records to Looker", saved_count)

        logger.info("Job completed successfully")
        return 0

    except Exception:
        logger.exception("Job failed with error")
        return 1

    finally:
        if repository is not None:
            await repository.disconnect()


def main() -> None:
    """Main entry point for Cloud Run Job."""
    setup_logging()

    settings = get_settings()
    days = settings.report_lookback_days

    logger.info("Starting Coupon Mention Tracker job")

    exit_code = asyncio.run(run_weekly_report(days=days, send_slack=True))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
