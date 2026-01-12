"""Unit tests for GoogleSheetsClient without hitting the network."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from coupon_mention_tracker.clients.google_sheets_client import (
    GoogleSheetsClient,
)


def _make_client_with_service(service) -> GoogleSheetsClient:
    client = object.__new__(GoogleSheetsClient)
    client._spreadsheet_id = "sheet"
    client._service = service
    client._sheet_metadata = None
    return client


def test_column_index_to_letter() -> None:
    client = object.__new__(GoogleSheetsClient)

    assert client._column_index_to_letter(0) == "A"
    assert client._column_index_to_letter(25) == "Z"
    assert client._column_index_to_letter(26) == "AA"
    assert client._column_index_to_letter(27) == "AB"


def test_get_sheet_title_by_gid_found() -> None:
    client = object.__new__(GoogleSheetsClient)
    client._sheet_metadata = {
        "sheets": [
            {"properties": {"sheetId": 0, "title": "Coupons"}},
            {"properties": {"sheetId": 123, "title": "Other"}},
        ]
    }

    assert client._get_sheet_title_by_gid(123) == "Other"
    assert client._get_sheet_title_by_gid(999) is None


def test_get_column_index_by_name_found_and_missing() -> None:
    class _Svc:
        def spreadsheets(self):
            return self

        def values(self):
            return self

        def get(self, spreadsheetId, range):
            self._last = (spreadsheetId, range)
            return self

        def execute(self):
            return {"values": [["A", "Coupon", "C"]]}

    client = _make_client_with_service(_Svc())

    assert client._get_column_index_by_name("Sheet1", "Coupon") == 1
    assert client._get_column_index_by_name("Sheet1", "Missing") is None


def test_get_column_values_by_gid_and_name_success_strips_and_skips_empty() -> (
    None
):
    calls = []

    class _Svc:
        def spreadsheets(self):
            return self

        def values(self):
            return self

        def get(self, spreadsheetId, range):
            calls.append((spreadsheetId, range))
            return self

        def execute(self):
            if calls[-1][1].endswith("!1:1"):
                return {"values": [["A", "Coupon"]]}
            return {"values": [["  SAVE10  "], [""], ["  "], ["NEW20"]]}

    client = _make_client_with_service(_Svc())
    client._sheet_metadata = {
        "sheets": [{"properties": {"sheetId": 0, "title": "Sheet1"}}]
    }

    values = client.get_column_values_by_gid_and_name(
        gid=0,
        column_name="Coupon",
        skip_header=True,
    )

    assert values == ["SAVE10", "NEW20"]


def test_get_column_values_by_gid_and_name_raises_when_sheet_missing() -> None:
    client = _make_client_with_service(SimpleNamespace())
    client._sheet_metadata = {"sheets": []}

    with pytest.raises(ValueError, match="Sheet with GID"):
        client.get_column_values_by_gid_and_name(
            gid=123,
            column_name="Coupon",
        )
