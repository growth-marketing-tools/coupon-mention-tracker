"""Slack notification service for sending coupon mention alerts."""

from collections import defaultdict
from datetime import date
from http import HTTPStatus

from slack_sdk.models.blocks import (
    Block,
    ContextBlock,
    DividerBlock,
    HeaderBlock,
    MarkdownTextObject,
    PlainTextObject,
    SectionBlock,
)
from slack_sdk.webhook.async_client import AsyncWebhookClient

from coupon_mention_tracker.core.models import CouponMatch, WeeklyReportRow


MAX_DISPLAY_ITEMS = 10


class SlackClient:
    """Service for sending Slack notifications about coupon mentions."""

    def __init__(
        self,
        webhook_url: str,
        default_channel: str = "#coupon-alerts",
    ) -> None:
        """Initialize Slack notifier.

        Args:
            webhook_url: Slack webhook URL.
            default_channel: Default channel for notifications.
        """
        self._webhook_url = webhook_url
        self._default_channel = default_channel
        self._client = AsyncWebhookClient(url=webhook_url)

    async def send_message(
        self,
        text: str,
        blocks: list[Block] | list[dict] | None = None,
    ) -> bool:
        """Send a message to Slack.

        Args:
            text: Fallback text for the message.
            blocks: Optional Block Kit blocks for rich formatting.

        Returns:
            True if message was sent successfully.
        """
        response = await self._client.send(
            text=text,
            blocks=blocks,
        )
        return response.status_code == HTTPStatus.OK and response.body == "ok"

    @staticmethod
    def _format_date_range(
        first_seen: date | None,
        last_seen: date | None,
    ) -> str:
        """Format a date range for display."""
        if first_seen is None and last_seen is None:
            return ""
        if first_seen and last_seen:
            if first_seen == last_seen:
                return f"({first_seen})"
            return f"({first_seen} to {last_seen})"
        if last_seen:
            return f"({last_seen})"
        if first_seen:
            return f"({first_seen})"
        return ""

    @staticmethod
    def _format_coupon_match_block(match: CouponMatch) -> Block:
        """Format a single coupon match as a Slack block."""
        location = match.location or "Global"
        return SectionBlock(
            text=MarkdownTextObject(
                text=(
                    f"*Keyword:* `{match.keyword}`\n"
                    f"*Location:* {location}\n"
                    f"*Product:* {match.product}\n"
                    f"*Coupon:* `{match.coupon_code}`\n"
                    f"*Date:* {match.scraped_date}\n"
                    f"*Context:* _{match.match_context}_"
                )
            )
        )

    @staticmethod
    def _group_rows_by_coupon(
        rows: list[WeeklyReportRow],
    ) -> list[tuple[str, list[WeeklyReportRow]]]:
        """Group report rows by coupon code and sort A to Z."""
        grouped: dict[str, list[WeeklyReportRow]] = defaultdict(list)
        for row in rows:
            if row.coupon_detected:
                grouped[row.coupon_detected].append(row)
        return sorted(grouped.items(), key=lambda item: item[0].casefold())

    @staticmethod
    def _format_coupon_group_text(
        coupon_code: str,
        rows: list[WeeklyReportRow],
    ) -> str:
        """Format grouped coupon rows for Slack display."""
        sorted_rows = sorted(
            rows,
            key=lambda row: (
                row.keyword.casefold(),
                (row.location or "").casefold(),
            ),
        )
        lines = [f"*`{coupon_code}`*"]
        for row in sorted_rows:
            location = row.location or "Global"
            date_range = SlackClient._format_date_range(
                row.first_seen, row.last_seen
            )
            date_suffix = f" {date_range}" if date_range else ""
            lines.append(f"• _{row.keyword}_ ({location}){date_suffix}")
        return "\n".join(lines)

    async def send_coupon_alert(
        self,
        matches: list[CouponMatch],
    ) -> bool:
        """Send an alert about detected coupon mentions.

        Args:
            matches: List of coupon matches to report.

        Returns:
            True if alert was sent successfully.
        """
        if not matches:
            return True

        blocks: list[Block] = [
            HeaderBlock(
                text=PlainTextObject(
                    text="Coupon mentions detected in AI Overviews"
                )
            ),
            ContextBlock(
                elements=[
                    MarkdownTextObject(
                        text=f"Found {len(matches)} coupon mention(s)"
                    )
                ]
            ),
            DividerBlock(),
        ]

        for match in matches[:MAX_DISPLAY_ITEMS]:
            blocks.append(self._format_coupon_match_block(match))
            blocks.append(DividerBlock())

        if len(matches) > MAX_DISPLAY_ITEMS:
            remaining = len(matches) - MAX_DISPLAY_ITEMS
            blocks.append(
                ContextBlock(
                    elements=[
                        MarkdownTextObject(text=f"_...and {remaining} more_")
                    ]
                )
            )

        return await self.send_message(
            text=f"Found {len(matches)} coupon mentions in AI Overviews",
            blocks=blocks,
        )

    def _build_weekly_report_blocks(
        self,
        report_rows: list[WeeklyReportRow],
        start_date: date,
        end_date: date,
    ) -> list[Block]:
        """Build Slack blocks for weekly report."""
        unique_keywords = {
            (row.keyword, row.location)
            for row in report_rows
            if row.has_ai_overview
        }
        with_coupons = [row for row in report_rows if row.coupon_detected]
        invalid_coupons = [
            row for row in with_coupons if row.is_valid_coupon is False
        ]

        blocks: list[Block] = [
            HeaderBlock(
                text=PlainTextObject(text="Weekly coupon mention report")
            ),
            ContextBlock(
                elements=[
                    MarkdownTextObject(
                        text=f"Period: {start_date} to {end_date}"
                    )
                ]
            ),
            DividerBlock(),
            SectionBlock(
                text=MarkdownTextObject(
                    text=(
                        f"*Summary*\n"
                        f"• Keywords analyzed: {len(unique_keywords)}\n"
                        f"• Coupon mentions found: {len(with_coupons)}"
                    )
                )
            ),
            DividerBlock(),
        ]

        if invalid_coupons:
            blocks.append(
                SectionBlock(
                    text=MarkdownTextObject(text="*Untracked coupons detected*")
                )
            )
            for coupon_code, grouped_rows in self._group_rows_by_coupon(
                invalid_coupons
            ):
                blocks.append(
                    SectionBlock(
                        text=MarkdownTextObject(
                            text=self._format_coupon_group_text(
                                coupon_code, grouped_rows
                            )
                        )
                    )
                )

        if with_coupons:
            if invalid_coupons:
                blocks.append(DividerBlock())
            blocks.append(
                SectionBlock(
                    text=MarkdownTextObject(text="*Valid coupon mentions*")
                )
            )
            valid = [row for row in with_coupons if row.is_valid_coupon is True]
            for coupon_code, grouped_rows in self._group_rows_by_coupon(valid):
                blocks.append(
                    SectionBlock(
                        text=MarkdownTextObject(
                            text=self._format_coupon_group_text(
                                coupon_code, grouped_rows
                            )
                        )
                    )
                )

        return blocks

    async def send_weekly_report(
        self,
        rows: list[WeeklyReportRow],
        start_date: date,
        end_date: date,
    ) -> bool:
        """Send the weekly coupon mention report.

        Args:
            rows: Report data rows.
            start_date: Start of reporting period.
            end_date: End of reporting period.

        Returns:
            True if report was sent successfully.
        """
        blocks = self._build_weekly_report_blocks(rows, start_date, end_date)

        return await self.send_message(
            text=f"Weekly coupon report: {start_date} to {end_date}",
            blocks=blocks,
        )
