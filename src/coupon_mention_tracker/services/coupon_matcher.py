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

    @staticmethod
    def _extract_context(
        text: str, start: int, end: int, context_chars: int
    ) -> str:
        """Extract text context around a match.

        Args:
            text: Full text content.
            start: Start position of match.
            end: End position of match.
            context_chars: Number of characters to include around match.

        Returns:
            Text snippet with context around the match.
        """
        context_start = max(0, start - context_chars)
        context_end = min(len(text), end + context_chars)

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
            for coupon_match in pattern.finditer(text):
                context = self._extract_context(
                    text,
                    coupon_match.start(),
                    coupon_match.end(),
                    self._context_chars,
                )
                matches.append(
                    MatchResult(
                        coupon_code=coupon,
                        start_pos=coupon_match.start(),
                        end_pos=coupon_match.end(),
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

        found_coupons = set()
        for coupon_pattern in coupon_patterns:
            pattern_matches = re.findall(coupon_pattern, text, re.IGNORECASE)
            found_coupons.update(match.upper() for match in pattern_matches)

        return list(found_coupons)

    def find_in_html_sources(
        self, sources: list[dict], coupon_code: str
    ) -> list[str]:
        """Find which source URLs contain the coupon in their HTML content.

        Args:
            sources: List of source dicts.
            coupon_code: The coupon code to search for.

        Returns:
            List of source URLs where the coupon was found.
        """
        urls_with_mention = []
        pattern = self._patterns.get(coupon_code.upper())

        if not pattern:
            return urls_with_mention

        for source in sources:
            html_content = source.get("source_html_content", "")
            if html_content and pattern.search(html_content):
                urls_with_mention.append(source["source_url"])

        return urls_with_mention

    def analyze_result(
        self,
        prompt: AIOverviewPrompt,
        result: AIOverviewResult,
        sources_with_html: list[dict] | None = None,
    ) -> list[CouponMatch]:
        """Analyze an AI Overview result for coupon mentions.

        Args:
            prompt: The prompt/keyword associated with the result.
            result: The AI Overview result to analyze.
            sources_with_html: Optional list of source dicts with HTML content.

        Returns:
            List of coupon matches found in the result.
        """
        if not result.response_text:
            return []

        matches = self.find_matches(result.response_text)
        has_sources = (
            sources_with_html is not None and len(sources_with_html) > 0
        )

        coupon_matches = []
        for match in matches:
            source_urls = []
            if has_sources and sources_with_html:
                source_urls = self.find_in_html_sources(
                    sources_with_html, match.coupon_code
                )

            coupon_matches.append(
                CouponMatch(
                    keyword=prompt.prompt_text,
                    location=prompt.location,
                    product=prompt.primary_product,
                    scraped_date=result.scraped_date,
                    coupon_code=match.coupon_code,
                    match_context=match.context,
                    ai_overview_id=result.id,
                    source_urls_with_mentions=source_urls,
                    source_mention_unavailable=not has_sources,
                )
            )

        return coupon_matches

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
