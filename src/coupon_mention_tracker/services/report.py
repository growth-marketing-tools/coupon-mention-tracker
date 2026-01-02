"""Weekly report generator for coupon mentions in AI Overviews."""

from collections import defaultdict
from datetime import date, timedelta

from coupon_mention_tracker.clients.slack import SlackNotifier
from coupon_mention_tracker.core.models import CouponMatch, WeeklyReportRow
from coupon_mention_tracker.repositories.ai_overview import (
    AIOverviewRepository,
)
from coupon_mention_tracker.services.coupon_matcher import CouponMatcher


class WeeklyReportGenerator:
    """Generates weekly coupon mention reports from AI Overview data."""

    def __init__(
        self,
        repository: AIOverviewRepository,
        matcher: CouponMatcher,
        notifier: SlackNotifier,
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
    ) -> tuple[list[WeeklyReportRow], list[CouponMatch]]:
        """Generate weekly report data.

        Args:
            days: Number of days to look back.

        Returns:
            Tuple of (report rows, all coupon matches found).
        """
        results = await self._repository.get_results_last_n_days(days=days)

        keyword_data: dict[tuple, dict] = defaultdict(
            lambda: {
                "has_ai_overview": False,
                "coupons": defaultdict(int),
                "last_seen": None,
                "product": "",
                "location": None,
            }
        )

        all_matches: list[CouponMatch] = []

        for prompt, result in results:
            key = (prompt.prompt_text, prompt.location)
            data = keyword_data[key]
            data["has_ai_overview"] = True
            data["product"] = prompt.primary_product
            data["location"] = prompt.location

            if (
                data["last_seen"] is None
                or result.scraped_date > data["last_seen"]
            ):
                data["last_seen"] = result.scraped_date

            matches = self._matcher.analyze_result(prompt, result)
            for match in matches:
                data["coupons"][match.coupon_code] += 1
                all_matches.append(match)

        rows: list[WeeklyReportRow] = []
        for (keyword, location), data in keyword_data.items():
            if data["coupons"]:
                top_coupon = max(
                    data["coupons"].items(),
                    key=lambda x: x[1],
                )
                coupon_code, count = top_coupon
                is_valid = self._matcher.is_valid_coupon(coupon_code)
            else:
                coupon_code = None
                count = 0
                is_valid = None

            rows.append(
                WeeklyReportRow(
                    keyword=keyword,
                    location=location,
                    product=data["product"],
                    has_ai_overview=data["has_ai_overview"],
                    coupon_detected=coupon_code,
                    is_valid_coupon=is_valid,
                    last_seen=data["last_seen"],
                    mention_count=count,
                )
            )

        rows.sort(key=lambda r: (r.coupon_detected is None, r.keyword))

        return rows, all_matches

    async def run_and_send(self, days: int = 7) -> bool:
        """Generate and send the weekly report.

        Args:
            days: Number of days to look back.

        Returns:
            True if report was sent successfully.
        """
        rows, _ = await self.generate_report(days=days)

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
    ) -> list[CouponMatch]:
        """Get matches for coupons not in the valid list.

        Args:
            days: Number of days to look back.

        Returns:
            List of matches with invalid/unknown coupons.
        """
        results = await self._repository.get_results_last_n_days(days=days)
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
