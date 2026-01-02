"""Coupon matching service for detecting coupons in AI Overview text."""

import re
from dataclasses import dataclass

from coupon_mention_tracker.core.models import (
    AIOverviewPrompt,
    AIOverviewResult,
    CouponMatch,
)


@dataclass
class MatchResult:
    """Result of a coupon match operation."""

    coupon_code: str
    start_pos: int
    end_pos: int
    context: str


class CouponMatcher:
    """Service for detecting coupon codes in AI Overview text."""

    def __init__(
        self,
        coupons: list[str],
        context_chars: int = 100,
    ) -> None:
        """Initialize coupon matcher.

        Args:
            coupons: List of coupon codes to search for.
            context_chars: Number of characters to include around match.
        """
        self._coupons = [c.strip().upper() for c in coupons if c.strip()]
        self._context_chars = context_chars
        self._patterns = self._build_patterns()

    def _build_patterns(self) -> dict[str, re.Pattern]:
        """Build regex patterns for each coupon code.

        Returns:
            Dictionary mapping coupon codes to compiled regex patterns.
        """
        patterns = {}
        for coupon in self._coupons:
            escaped = re.escape(coupon)
            pattern = re.compile(
                rf"\b{escaped}\b",
                re.IGNORECASE,
            )
            patterns[coupon] = pattern
        return patterns

    def _extract_context(self, text: str, start: int, end: int) -> str:
        """Extract text context around a match.

        Args:
            text: Full text content.
            start: Start position of match.
            end: End position of match.

        Returns:
            Text snippet with context around the match.
        """
        context_start = max(0, start - self._context_chars)
        context_end = min(len(text), end + self._context_chars)

        prefix = "..." if context_start > 0 else ""
        suffix = "..." if context_end < len(text) else ""

        snippet = text[context_start:context_end].strip()
        snippet = " ".join(snippet.split())

        return f"{prefix}{snippet}{suffix}"

    def find_matches(self, text: str) -> list[MatchResult]:
        """Find all coupon matches in text.

        Args:
            text: Text content to search.

        Returns:
            List of match results with coupon codes and context.
        """
        if not text:
            return []

        matches = []
        for coupon, pattern in self._patterns.items():
            for match in pattern.finditer(text):
                context = self._extract_context(
                    text,
                    match.start(),
                    match.end(),
                )
                matches.append(
                    MatchResult(
                        coupon_code=coupon,
                        start_pos=match.start(),
                        end_pos=match.end(),
                        context=context,
                    )
                )

        return matches

    def find_any_coupon_pattern(self, text: str) -> list[str]:
        """Find any coupon-like patterns in text (not just tracked ones).

        This helps identify coupons that may not be in the tracked list.

        Args:
            text: Text content to search.

        Returns:
            List of potential coupon codes found.
        """
        if not text:
            return []

        coupon_patterns = [
            r"\b[A-Z]{2,}[0-9]{2,}\b",
            r"\b[A-Z]{3,}(?:OFF|SAVE|DEAL|VPN|PASS)\b",
            r"\bcoupon[:\s]+[A-Z0-9]+\b",
            r"\bcode[:\s]+[A-Z0-9]+\b",
            r"\bpromo[:\s]+[A-Z0-9]+\b",
        ]

        found = set()
        for pattern in coupon_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            found.update(m.upper() for m in matches)

        return list(found)

    def analyze_result(
        self,
        prompt: AIOverviewPrompt,
        result: AIOverviewResult,
    ) -> list[CouponMatch]:
        """Analyze an AI Overview result for coupon mentions.

        Args:
            prompt: The prompt/keyword associated with the result.
            result: The AI Overview result to analyze.

        Returns:
            List of coupon matches found in the result.
        """
        if not result.response_text:
            return []

        matches = self.find_matches(result.response_text)

        return [
            CouponMatch(
                keyword=prompt.prompt_text,
                location=prompt.location,
                product=prompt.primary_product,
                scraped_date=result.scraped_date,
                coupon_code=match.coupon_code,
                match_context=match.context,
                ai_overview_id=result.id,
            )
            for match in matches
        ]

    @property
    def tracked_coupons(self) -> list[str]:
        """Get list of tracked coupon codes."""
        return self._coupons.copy()

    def is_valid_coupon(self, code: str) -> bool:
        """Check if a coupon code is in the tracked list.

        Args:
            code: Coupon code to check.

        Returns:
            True if coupon is tracked, False otherwise.
        """
        return code.upper() in self._coupons
