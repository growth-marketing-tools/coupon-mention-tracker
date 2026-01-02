"""Slack notification service for sending coupon mention alerts."""

from datetime import date
from http import HTTPStatus

import httpx

from coupon_mention_tracker.core.models import CouponMatch, WeeklyReportRow


MAX_DISPLAY_ITEMS = 10


class SlackNotifier:
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

    async def send_message(
        self,
        text: str,
        blocks: list[dict] | None = None,
    ) -> bool:
        """Send a message to Slack.

        Args:
            text: Fallback text for the message.
            blocks: Optional Block Kit blocks for rich formatting.

        Returns:
            True if message was sent successfully.
        """
        payload: dict = {"text": text}
        if blocks:
            payload["blocks"] = blocks

        async with httpx.AsyncClient() as client:
            response = await client.post(
                self._webhook_url,
                json=payload,
                timeout=30.0,
            )
            return response.status_code == HTTPStatus.OK

    def _format_coupon_match_block(self, match: CouponMatch) -> dict:
        """Format a single coupon match as a Slack block."""
        location = match.location or "Global"
        return {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*Keyword:* `{match.keyword}`\n"
                    f"*Location:* {location}\n"
                    f"*Product:* {match.product}\n"
                    f"*Coupon:* `{match.coupon_code}`\n"
                    f"*Date:* {match.scraped_date}\n"
                    f"*Context:* _{match.match_context}_"
                ),
            },
        }

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

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "🎟️ Coupon Mentions Detected in AI Overviews",
                },
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Found {len(matches)} coupon mention(s)",
                    }
                ],
            },
            {"type": "divider"},
        ]

        for match in matches[:MAX_DISPLAY_ITEMS]:
            blocks.append(self._format_coupon_match_block(match))
            blocks.append({"type": "divider"})

        if len(matches) > MAX_DISPLAY_ITEMS:
            remaining = len(matches) - MAX_DISPLAY_ITEMS
            blocks.append(
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"_...and {remaining} more_",
                        }
                    ],
                }
            )

        return await self.send_message(
            text=f"Found {len(matches)} coupon mentions in AI Overviews",
            blocks=blocks,
        )

    def _build_weekly_report_blocks(
        self,
        rows: list[WeeklyReportRow],
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """Build Slack blocks for weekly report."""
        with_coupons = [r for r in rows if r.coupon_detected]
        invalid_coupons = [
            r for r in with_coupons if r.is_valid_coupon is False
        ]

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "📊 Weekly Coupon Mention Report",
                },
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Period: {start_date} to {end_date}",
                    }
                ],
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*Summary*\n"
                        f"• Keywords tracked: {len(rows)}\n"
                        f"• Keywords with AI Overview: "
                        f"{sum(1 for r in rows if r.has_ai_overview)}\n"
                        f"• Coupon mentions found: {len(with_coupons)}\n"
                        f"• Invalid/outdated coupons: {len(invalid_coupons)}"
                    ),
                },
            },
            {"type": "divider"},
        ]

        if invalid_coupons:
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*⚠️ Invalid Coupons Detected:*",
                    },
                }
            )
            for row in invalid_coupons[:5]:
                location = row.location or "Global"
                blocks.append(
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": (
                                f"• `{row.coupon_detected}` in "
                                f"_{row.keyword}_ ({location})"
                            ),
                        },
                    }
                )

        if with_coupons:
            blocks.append({"type": "divider"})
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*✅ Valid Coupon Mentions:*",
                    },
                }
            )
            valid = [r for r in with_coupons if r.is_valid_coupon is True]
            for row in valid[:10]:
                location = row.location or "Global"
                blocks.append(
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": (
                                f"• `{row.coupon_detected}` in "
                                f"_{row.keyword}_ ({location}) - "
                                f"seen {row.mention_count}x"
                            ),
                        },
                    }
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
            text=f"Weekly Coupon Report: {start_date} to {end_date}",
            blocks=blocks,
        )
