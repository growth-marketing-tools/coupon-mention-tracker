"""Weekly report generator for coupon mentions in AI Overviews."""

from collections import defaultdict
from datetime import date, timedelta
from typing import Protocol
from uuid import UUID

from coupon_mention_tracker.clients.slack import SlackClient
from coupon_mention_tracker.core.models import (
    AIOverviewPrompt,
    AIOverviewResult,
    CouponMatch,
    WeeklyReportRow,
)
from coupon_mention_tracker.services.coupon_matcher import CouponMatcher


class _AIOverviewRepositoryLike(Protocol):
    """The minimal repository API needed by WeeklyReportGenerator."""

    async def get_results_last_n_days(
        self,
        days: int = 7,
        provider: str = "google_ai_overview",
        tags: list[str] | None = None,
    ) -> list[tuple[AIOverviewPrompt, AIOverviewResult]]: ...

    async def get_sources_with_html(
        self, result_ids: list[str | UUID]
    ) -> dict[str, list[dict]]: ...


class WeeklyReportGenerator:
    """Generates weekly coupon mention reports from AI Overview data."""

    def __init__(
        self,
        repository: _AIOverviewRepositoryLike,
        matcher: CouponMatcher,
        notifier: SlackClient,
    ) -> None:
        """Initialize report generator.

        Args:
            repository: Repository for fetching AI Overview data.
            matcher: Coupon matcher for detecting coupons.
            notifier: Slack notifier for sending reports.
        """
        self._repository = repository
        self._matcher = matcher
        self._notifier = notifier

    async def generate_report(
        self,
        days: int = 7,
        tags: list[str] | None = None,
    ) -> tuple[list[WeeklyReportRow], list[CouponMatch]]:
        """Generate weekly report data.

        Args:
            days: Number of days to look back.
            tags: Filter by tags (e.g., ['Dominykas']).

        Returns:
            Tuple of (report rows, all coupon matches found).
        """
        results = await self._repository.get_results_last_n_days(
            days=days, tags=tags
        )

        result_ids: list[str | UUID] = [result.id for _, result in results]
        sources_by_result = await self._repository.get_sources_with_html(
            result_ids
        )

        keyword_data: dict[tuple, dict] = defaultdict(
            lambda: {
                "has_ai_overview": False,
                "coupons": defaultdict(
                    lambda: {"count": 0, "first_seen": None, "last_seen": None}
                ),
                "product": "",
                "location": None,
            }
        )

        all_matches: list[CouponMatch] = []

        for prompt, result in results:
            keyword_key = (prompt.prompt_text, prompt.location)
            data = keyword_data[keyword_key]
            data["has_ai_overview"] = True
            data["product"] = prompt.primary_product
            data["location"] = prompt.location

            sources = sources_by_result.get(str(result.id))

            matches = self._matcher.analyze_result(prompt, result, sources)
            for coupon_match in matches:
                coupon_data = data["coupons"][coupon_match.coupon_code]
                coupon_data["count"] += 1

                if (
                    coupon_data["first_seen"] is None
                    or result.scraped_date < coupon_data["first_seen"]
                ):
                    coupon_data["first_seen"] = result.scraped_date

                if (
                    coupon_data["last_seen"] is None
                    or result.scraped_date > coupon_data["last_seen"]
                ):
                    coupon_data["last_seen"] = result.scraped_date

                all_matches.append(coupon_match)

        rows: list[WeeklyReportRow] = []
        for (keyword, location), data in keyword_data.items():
            if data["coupons"]:
                for coupon_code, coupon_info in data["coupons"].items():
                    rows.append(
                        WeeklyReportRow(
                            keyword=keyword,
                            location=location,
                            product=data["product"],
                            has_ai_overview=data["has_ai_overview"],
                            coupon_detected=coupon_code,
                            is_valid_coupon=self._matcher.is_valid_coupon(
                                coupon_code
                            ),
                            first_seen=coupon_info["first_seen"],
                            last_seen=coupon_info["last_seen"],
                            mention_count=coupon_info["count"],
                        )
                    )
            else:
                rows.append(
                    WeeklyReportRow(
                        keyword=keyword,
                        location=location,
                        product=data["product"],
                        has_ai_overview=data["has_ai_overview"],
                        coupon_detected=None,
                        is_valid_coupon=None,
                        first_seen=None,
                        last_seen=None,
                        mention_count=0,
                    )
                )

        rows.sort(
            key=lambda row: (
                row.keyword,
                row.location or "",
                row.coupon_detected is None,
                row.coupon_detected or "",
            )
        )

        return rows, all_matches

    async def run_and_send(
        self, days: int = 7, tags: list[str] | None = None
    ) -> bool:
        """Generate and send the weekly report.

        Args:
            days: Number of days to look back.
            tags: Filter by tags (e.g., ['Dominykas']).

        Returns:
            True if report was sent successfully.
        """
        rows, _ = await self.generate_report(days=days, tags=tags)

        end_date = date.today()
        start_date = end_date - timedelta(days=days)

        return await self._notifier.send_weekly_report(
            rows=rows,
            start_date=start_date,
            end_date=end_date,
        )

    async def get_invalid_coupon_alerts(
        self,
        days: int = 7,
        tags: list[str] | None = None,
    ) -> list[CouponMatch]:
        """Get matches for coupons not in the valid list.

        Args:
            days: Number of days to look back.
            tags: Filter by tags (e.g., ['Dominykas']).

        Returns:
            List of matches with invalid/unknown coupons.
        """
        results = await self._repository.get_results_last_n_days(
            days=days, tags=tags
        )
        invalid_matches: list[CouponMatch] = []

        for prompt, result in results:
            if not result.response_text:
                continue

            potential = self._matcher.find_any_coupon_pattern(
                result.response_text
            )

            for code in potential:
                if not self._matcher.is_valid_coupon(code):
                    invalid_matches.append(
                        CouponMatch(
                            keyword=prompt.prompt_text,
                            location=prompt.location,
                            product=prompt.primary_product,
                            scraped_date=result.scraped_date,
                            coupon_code=code,
                            match_context="[Untracked coupon pattern]",
                            ai_overview_id=result.id,
                        )
                    )

        return invalid_matches
