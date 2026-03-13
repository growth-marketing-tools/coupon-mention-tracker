"""Looker API client for fetching coupon performance data."""

from __future__ import annotations

from typing import Any

import httpx
from loguru import logger

from coupon_mention_tracker.core.models import (
    CouponPerformance,
    CouponPerformanceTrend,
)


_API_PREFIX = "/api/4.0"
_LOGIN_PATH = f"{_API_PREFIX}/login"
_QUERY_RUN_PATH = f"{_API_PREFIX}/queries/run/json"
_HTTP_UNAUTHORIZED = 401

# NordSec explore (NordVPN, NordPass, NordLocker, NordProtect)
_NORDSEC_MODEL = "tesonet"
_NORDSEC_VIEW = "prod_core_nordsec"
_NORDSEC_COUPON = "prod_core__orders_data_nordsec.coupon_code"
_NORDSEC_REVENUE = (
    "prod_core__fct_payments_nordsec.total_nordsec_order_billings_in_usd"
)
_NORDSEC_TRANSACTIONS = (
    "prod_core__fct_payments_nordsec.count_nordsec_transactions"
)
_NORDSEC_DATE = "prod_core__fct_payments_nordsec.payment_created_date"
_NORDSEC_FIRST_RECURRING = (
    "prod_core__products_data_nordsec.product_first_or_recurring"
)

# Saily explore
_SAILY_MODEL = "tesonet"
_SAILY_VIEW = "prod_core_esim"
_SAILY_COUPON = "prod_core_esim.order_coupon_code"
_SAILY_REVENUE = "prod_core_esim.total_billings_in_usd"
_SAILY_TRANSACTIONS = "prod_core_esim.number_of_transactions"
_SAILY_DATE = "prod_core_esim.payment_created_date"
_SAILY_FIRST_RECURRING = "prod_core_esim.first_or_recurring_with_refunds"


class LookerClient:
    """Client for querying coupon performance via Looker API."""

    def __init__(
        self,
        base_url: str,
        client_id: str,
        client_secret: str,
    ) -> None:
        """Initialize with Looker credentials.

        Args:
            base_url: Looker instance URL.
            client_id: API3 client ID.
            client_secret: API3 client secret.
        """
        self._base_url = base_url.rstrip("/")
        self._client_id = client_id
        self._client_secret = client_secret
        self._http = httpx.AsyncClient(timeout=60)
        self._token: str | None = None

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        await self._http.aclose()

    async def _authenticate(self) -> None:
        """Obtain an access token from the Looker API."""
        resp = await self._http.post(
            f"{self._base_url}{_LOGIN_PATH}",
            data={
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            },
        )
        resp.raise_for_status()
        self._token = resp.json()["access_token"]

    async def _run_inline_query(
        self,
        model: str,
        view: str,
        fields: list[str],
        filters: dict[str, str],
    ) -> list[dict[str, Any]]:
        """Run an inline query and return result rows.

        Args:
            model: Looker model name.
            view: Looker explore name.
            fields: List of field names to select.
            filters: Filter name-value pairs.

        Returns:
            List of result row dicts.
        """
        if self._token is None:
            await self._authenticate()

        body = {
            "model": model,
            "view": view,
            "fields": fields,
            "filters": filters,
            "limit": "5000",
        }
        headers = {"Authorization": f"token {self._token}"}

        resp = await self._http.post(
            f"{self._base_url}{_QUERY_RUN_PATH}",
            json=body,
            headers=headers,
        )

        if resp.status_code == _HTTP_UNAUTHORIZED:
            logger.info("[LOOKER] Token expired, re-authenticating")
            await self._authenticate()
            headers = {"Authorization": f"token {self._token}"}
            resp = await self._http.post(
                f"{self._base_url}{_QUERY_RUN_PATH}",
                json=body,
                headers=headers,
            )

        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def _parse_performance_rows(
        rows: list[dict[str, Any]],
        coupon_field: str,
        revenue_field: str,
        transactions_field: str,
    ) -> dict[str, CouponPerformance]:
        """Parse Looker query rows into CouponPerformance.

        Args:
            rows: Raw rows from Looker API.
            coupon_field: Field name for coupon code.
            revenue_field: Field name for revenue.
            transactions_field: Field name for transactions.

        Returns:
            Dict mapping coupon code to aggregated data.
        """
        result: dict[str, CouponPerformance] = {}
        for row in rows:
            code = row.get(coupon_field)
            if not code:
                continue
            code_upper = code.upper()
            revenue = float(row.get(revenue_field) or 0)
            txns = int(row.get(transactions_field) or 0)

            if code_upper in result:
                existing = result[code_upper]
                result[code_upper] = CouponPerformance(
                    coupon_code=code_upper,
                    total_revenue_usd=(existing.total_revenue_usd + revenue),
                    total_transactions=(existing.total_transactions + txns),
                )
            else:
                result[code_upper] = CouponPerformance(
                    coupon_code=code_upper,
                    total_revenue_usd=revenue,
                    total_transactions=txns,
                )
        return result

    async def _fetch_period_performance(
        self,
        coupon_filter: str,
        date_filter: str,
    ) -> dict[str, CouponPerformance]:
        """Fetch combined performance for a date period.

        Args:
            coupon_filter: Comma-separated coupon codes.
            date_filter: Looker date filter expression.

        Returns:
            Dict mapping coupon code to merged performance.
        """
        merged: dict[str, CouponPerformance] = {}

        for label, is_nordsec in [
            ("NordSec", True),
            ("Saily", False),
        ]:
            try:
                result = await self._fetch_explore_performance(
                    coupon_filter=coupon_filter,
                    date_filter=date_filter,
                    is_nordsec=is_nordsec,
                )
                for code, perf in result.items():
                    if code in merged:
                        ex = merged[code]
                        merged[code] = CouponPerformance(
                            coupon_code=code,
                            total_revenue_usd=(
                                ex.total_revenue_usd
                                + perf.total_revenue_usd
                            ),
                            total_transactions=(
                                ex.total_transactions
                                + perf.total_transactions
                            ),
                        )
                    else:
                        merged[code] = perf
                logger.info(
                    "[LOOKER] %s (%s): %d results",
                    label,
                    date_filter,
                    len(result),
                )
            except Exception:
                logger.exception(
                    "[LOOKER] Failed %s (%s)",
                    label,
                    date_filter,
                )

        return merged

    async def _fetch_explore_performance(
        self,
        coupon_filter: str,
        date_filter: str,
        *,
        is_nordsec: bool,
    ) -> dict[str, CouponPerformance]:
        """Fetch performance from a single explore.

        Args:
            coupon_filter: Comma-separated coupon codes.
            date_filter: Looker date filter expression.
            is_nordsec: True for NordSec, False for Saily.

        Returns:
            Dict mapping coupon code to performance data.
        """
        if is_nordsec:
            rows = await self._run_inline_query(
                model=_NORDSEC_MODEL,
                view=_NORDSEC_VIEW,
                fields=[
                    _NORDSEC_COUPON,
                    _NORDSEC_REVENUE,
                    _NORDSEC_TRANSACTIONS,
                ],
                filters={
                    _NORDSEC_DATE: date_filter,
                    _NORDSEC_FIRST_RECURRING: "first",
                    _NORDSEC_COUPON: coupon_filter,
                },
            )
            return self._parse_performance_rows(
                rows,
                coupon_field=_NORDSEC_COUPON,
                revenue_field=_NORDSEC_REVENUE,
                transactions_field=_NORDSEC_TRANSACTIONS,
            )

        rows = await self._run_inline_query(
            model=_SAILY_MODEL,
            view=_SAILY_VIEW,
            fields=[
                _SAILY_COUPON,
                _SAILY_REVENUE,
                _SAILY_TRANSACTIONS,
            ],
            filters={
                _SAILY_DATE: date_filter,
                _SAILY_FIRST_RECURRING: (
                    '"first_with_refunds"'
                ),
                _SAILY_COUPON: coupon_filter,
            },
        )
        return self._parse_performance_rows(
            rows,
            coupon_field=_SAILY_COUPON,
            revenue_field=_SAILY_REVENUE,
            transactions_field=_SAILY_TRANSACTIONS,
        )

    async def get_coupon_performance_trend(
        self,
        coupon_codes: list[str],
    ) -> dict[str, CouponPerformanceTrend]:
        """Fetch week-over-week coupon performance trends.

        Queries this week (last 7 days) and previous week
        (8-14 days ago), then builds trend comparison.

        Args:
            coupon_codes: Coupon codes to look up.

        Returns:
            Dict mapping coupon code to trend data.
        """
        if not coupon_codes:
            return {}

        coupon_filter = ",".join(coupon_codes)

        this_week = await self._fetch_period_performance(
            coupon_filter, "7 days"
        )
        prev_week = await self._fetch_period_performance(
            coupon_filter, "14 days ago for 7 days"
        )

        all_codes = set(this_week) | set(prev_week)
        trends: dict[str, CouponPerformanceTrend] = {}

        for code in all_codes:
            tw = this_week.get(code)
            pw = prev_week.get(code)
            trends[code] = CouponPerformanceTrend(
                coupon_code=code,
                this_week_revenue=(
                    tw.total_revenue_usd if tw else 0.0
                ),
                this_week_transactions=(
                    tw.total_transactions if tw else 0
                ),
                prev_week_revenue=(
                    pw.total_revenue_usd if pw else 0.0
                ),
                prev_week_transactions=(
                    pw.total_transactions if pw else 0
                ),
            )

        return trends
