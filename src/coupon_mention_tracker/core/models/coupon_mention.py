"""Data models for coupon mentions and AI Overview tracking."""

import json
from datetime import date, datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class AIOverviewPrompt(BaseModel):
    """Represents a tracked keyword/prompt in the AI Overview system."""

    id: UUID
    prompt_text: str
    primary_product: str
    location: str | None = None
    status: str | None = None
    tags: list[str] | None = None
    created_at: datetime | None = None


class AIOverviewResult(BaseModel):
    """Represents an AI Overview result containing response text."""

    id: UUID
    prompt_id: UUID
    provider: str
    scraped_date: date
    scraped_at: datetime | None = None
    response_text: str | None = None
    sources: list[dict] | None = None
    ahrefs_volume: int | None = None
    sentiment_label: str | None = None

    @field_validator("sources", mode="before")
    @classmethod
    def parse_sources(cls, value: Any) -> list[dict] | None:
        """Parse sources from string if needed."""
        if value is None:
            return None
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                return parsed if isinstance(parsed, list) else None
            except (json.JSONDecodeError, TypeError):
                return None
        return value


class CouponMatch(BaseModel):
    """Represents a detected coupon mention in an AI Overview."""

    keyword: str = Field(description="The search keyword/prompt")
    location: str | None = Field(description="Country/location of the search")
    product: str = Field(description="Product associated with the keyword")
    scraped_date: date = Field(description="Date the AI Overview was scraped")
    coupon_code: str = Field(description="The coupon code that was detected")
    match_context: str = Field(
        description="Text snippet showing the coupon in context"
    )
    ai_overview_id: UUID = Field(description="ID of the AI Overview result")
    source_urls_with_mentions: list[str] = Field(
        default_factory=list,
        description="URLs of sources where coupon was found in HTML content",
    )
    source_mention_unavailable: bool = Field(
        default=False,
        description="True if we could not check sources (no HTML content)",
    )


class CouponPerformance(BaseModel):
    """Revenue and transaction data for a coupon code."""

    coupon_code: str = Field(
        description="The coupon code",
    )
    total_revenue_usd: float = Field(
        default=0.0,
        description="Total first-purchase revenue in USD",
    )
    total_transactions: int = Field(
        default=0,
        description="Total first-purchase transaction count",
    )


class CouponPerformanceTrend(BaseModel):
    """Week-over-week coupon performance comparison."""

    coupon_code: str = Field(
        description="The coupon code",
    )
    this_week_revenue: float = Field(default=0.0)
    this_week_transactions: int = Field(default=0)
    prev_week_revenue: float = Field(default=0.0)
    prev_week_transactions: int = Field(default=0)

    @property
    def revenue_change(self) -> float:
        """Absolute revenue change."""
        return self.this_week_revenue - self.prev_week_revenue

    @property
    def revenue_change_pct(self) -> float | None:
        """Percentage revenue change, None if no prior data."""
        if self.prev_week_revenue == 0:
            return None
        return (
            self.revenue_change / self.prev_week_revenue
        ) * 100


class WeeklyReportRow(BaseModel):
    """A single row in the weekly coupon report."""

    keyword: str
    location: str | None
    product: str
    has_ai_overview: bool = Field(description="Whether AI Overview was present")
    coupon_detected: str | None = Field(
        description="Coupon code if detected, None otherwise"
    )
    is_valid_coupon: bool | None = Field(
        description="Whether detected coupon is in active list"
    )
    first_seen: date | None = Field(description="First scrape date in period")
    last_seen: date | None = Field(description="Most recent scrape date")
    mention_count: int = Field(
        default=0, description="Number of times coupon was seen this period"
    )
