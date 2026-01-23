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
from coupon_mention_tracker.repositories.looker_repository import (
    LookerRepository,
)
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
    for coupon_match in matches:
        match_key = (
            coupon_match.keyword,
            coupon_match.location,
            coupon_match.scraped_date,
        )
        # Keep first match per keyword/location/date (or could aggregate)
        if match_key not in match_index:
            match_index[match_key] = coupon_match

    records = []
    for prompt, result in results:
        match_key = (prompt.prompt_text, prompt.location, result.scraped_date)
        coupon_match = match_index.get(match_key)

        records.append(
            {
                "keyword": prompt.prompt_text,
                "location": prompt.location,
                "primary_product": prompt.primary_product,
                "has_ai_overview": True,  # AI Overview exists
                "ai_overview_result_id": result.id,
                "tracked_coupon_present": coupon_match is not None,
                "detected_coupon_code": (
                    coupon_match.coupon_code if coupon_match else None
                ),
                "is_valid_coupon": (
                    matcher.is_valid_coupon(coupon_match.coupon_code)
                    if coupon_match
                    else None
                ),
                "match_context": (
                    coupon_match.match_context if coupon_match else None
                ),
                "scraped_date": result.scraped_date,
                "source_mention_count": (
                    len(coupon_match.source_urls_with_mentions)
                    if coupon_match
                    else 0
                ),
                "source_urls_with_mentions": (
                    coupon_match.source_urls_with_mentions
                    if coupon_match
                    else []
                ),
                "source_mention_unavailable": (
                    coupon_match.source_mention_unavailable
                    if coupon_match
                    else False
                ),
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
        logger.info("[GOOGLE_SHEETS] Tracking %d coupon codes", len(coupons))
        if not coupons:
            logger.warning(
                "[GOOGLE_SHEETS] No coupon codes found; "
                "matcher will never match"
            )

        repository = AIOverviewRepository(settings)
        matcher = CouponMatcher(coupons)
        notifier = SlackNotifier(
            webhook_url=settings.slack_webhook_url,
            default_channel=settings.slack_channel,
        )
        generator = WeeklyReportGenerator(repository, matcher, notifier)

        logger.info("[DATABASE] Connecting to database...")
        await repository.connect()

        tags_filter = ["Dominykas"]
        logger.info(
            "[REPORT] Generating coupon mention report for last %d days "
            "(tags: %s)",
            days,
            tags_filter,
        )
        rows, matches = await generator.generate_report(
            days=days, tags=tags_filter
        )

        raw_results = await repository.get_results_last_n_days(
            days=days, tags=tags_filter
        )

        end_date = date.today()
        start_date = end_date - timedelta(days=days)

        unique_keywords = {(row.keyword, row.location) for row in rows}
        keywords_with_overview = {
            (row.keyword, row.location) for row in rows if row.has_ai_overview
        }
        logger.info(
            "[REPORT] Report generated: %d keywords, %d with AI Overview, "
            "%d matches",
            len(unique_keywords),
            len(keywords_with_overview),
            len(matches),
        )

        with_coupons = [row for row in rows if row.coupon_detected]
        invalid_coupons = [
            row for row in with_coupons if row.is_valid_coupon is False
        ]

        if invalid_coupons:
            logger.warning(
                "[REPORT] Found %d invalid/outdated coupons in AI Overviews",
                len(invalid_coupons),
            )
            for row in invalid_coupons:
                logger.warning(
                    "[REPORT]  - %s in '%s' (%s)",
                    row.coupon_detected,
                    row.keyword,
                    row.location or "Global",
                )

        if send_slack:
            logger.info("[SLACK] Sending report to Slack...")
            success = await notifier.send_weekly_report(
                rows=rows,
                start_date=start_date,
                end_date=end_date,
            )
            if success:
                logger.info("[SLACK] Report sent to Slack successfully")
            else:
                logger.error(
                    "[SLACK] Error in send_weekly_report: "
                    "Failed to send report to Slack"
                )
                return 1

        logger.info("[LOOKER] Saving tracking data to Looker schema...")
        tracking_records = build_tracking_records(raw_results, matches, matcher)
        if tracking_records and repository._pool:
            looker_repo = LookerRepository(repository._pool)
            saved_count = await looker_repo.save_tracking_batch(
                tracking_records
            )
            logger.info(
                "[LOOKER] Saved %d tracking records to Looker",
                saved_count,
            )

        logger.info("[MAIN] Job completed successfully")
        return 0

    except Exception:
        logger.exception("[MAIN] Job failed with error")
        return 1

    finally:
        if repository is not None:
            await repository.disconnect()


def main() -> None:
    """Main entry point for Cloud Run Job."""
    setup_logging()

    settings = get_settings()
    days = settings.report_lookback_days

    logger.info("[MAIN] Starting Coupon Mention Tracker job")

    exit_code = asyncio.run(run_weekly_report(days=days, send_slack=True))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
