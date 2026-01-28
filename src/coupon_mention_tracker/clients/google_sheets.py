"""Google Sheets client for fetching coupon data using native API."""

import json
from typing import Any

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from coupon_mention_tracker.core.logger import get_logger


logger = get_logger(__name__)

GOOGLE_API_SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


class GoogleSheetsClient:
    """Client for fetching data from Google Sheets using native API."""

    def __init__(
        self,
        spreadsheet_id: str,
        credentials_json: str,
    ) -> None:
        """Initialize Google Sheets client.

        Args:
            spreadsheet_id: The Google Sheets spreadsheet ID.
            credentials_json: Service account credentials as JSON string.
        """
        self._spreadsheet_id = spreadsheet_id
        self._service = self._build_service(credentials_json)
        self._sheet_metadata: dict[str, Any] | None = None

    @staticmethod
    def _build_service(credentials_json: str) -> Any:
        """Build the Google Sheets API service.

        Args:
            credentials_json: Service account credentials as JSON string.

        Returns:
            Google Sheets API service resource.
        """
        credentials_info = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(
            credentials_info,
            scopes=GOOGLE_API_SCOPES,
        )
        return build("sheets", "v4", credentials=credentials)

    def _get_sheet_metadata(self) -> dict[str, Any]:
        """Fetch and cache spreadsheet metadata.

        Returns:
            Spreadsheet metadata including sheet properties.
        """
        if self._sheet_metadata is None:
            self._sheet_metadata = (
                self._service.spreadsheets()
                .get(spreadsheetId=self._spreadsheet_id)
                .execute()
            )
        return self._sheet_metadata

    def _get_sheet_title_by_gid(self, gid: int) -> str | None:
        """Get sheet title by GID.

        Args:
            gid: The sheet GID (sheetId).

        Returns:
            Sheet title if found, None otherwise.
        """
        metadata = self._get_sheet_metadata()
        for sheet in metadata.get("sheets", []):
            properties = sheet.get("properties", {})
            if properties.get("sheetId") == gid:
                return properties.get("title")
        return None

    def _get_column_index_by_name(
        self,
        sheet_title: str,
        column_name: str,
    ) -> int | None:
        """Get column index by header name.

        Args:
            sheet_title: The sheet title.
            column_name: The column header name to find.

        Returns:
            Column index (0-based) if found, None otherwise.
        """
        range_notation = f"'{sheet_title}'!1:1"
        result = (
            self._service.spreadsheets()
            .values()
            .get(spreadsheetId=self._spreadsheet_id, range=range_notation)
            .execute()
        )

        headers = result.get("values", [[]])[0]
        try:
            return headers.index(column_name)
        except ValueError:
            return None

    @staticmethod
    def _column_index_to_letter(column_index: int) -> str:
        """Convert column index to letter notation.

        Args:
            column_index: Column index (0-based).

        Returns:
            Column letter (A, B, ..., Z, AA, AB, etc.).
        """
        result = ""
        column_index += 1
        while column_index > 0:
            column_index -= 1
            result = chr(column_index % 26 + ord("A")) + result
            column_index //= 26
        return result

    def get_column_values_by_gid_and_name(
        self,
        gid: int,
        column_name: str,
        skip_header: bool = True,
    ) -> list[str]:
        """Fetch column values by sheet GID and column name.

        Args:
            gid: The sheet GID (sheetId).
            column_name: The column header name.
            skip_header: Whether to skip the header row.

        Returns:
            List of non-empty string values from the column.

        Raises:
            ValueError: If sheet or column not found.
            HttpError: If API request fails.
        """
        sheet_title = self._get_sheet_title_by_gid(gid)
        if sheet_title is None:
            msg = f"Sheet with GID {gid} not found"
            raise ValueError(msg)

        column_index = self._get_column_index_by_name(sheet_title, column_name)
        if column_index is None:
            msg = f"Column '{column_name}' not found in sheet '{sheet_title}'"
            raise ValueError(msg)

        column_letter = self._column_index_to_letter(column_index)
        start_row = 2 if skip_header else 1
        range_notation = (
            f"'{sheet_title}'!{column_letter}{start_row}:{column_letter}"
        )

        try:
            result = (
                self._service.spreadsheets()
                .values()
                .get(spreadsheetId=self._spreadsheet_id, range=range_notation)
                .execute()
            )
        except HttpError as error:
            logger.error(
                "[GOOGLE_SHEETS] Error in get_column_values: "
                "Failed to fetch values from range %s in spreadsheet %s: %s",
                range_notation,
                self._spreadsheet_id,
                error,
            )
            raise

        rows = result.get("values", [])
        return [row[0].strip() for row in rows if row and row[0].strip()]

    def get_coupons(
        self,
        gid: int,
        column_name: str = "Coupon",
    ) -> list[str]:
        """Fetch coupon codes from the specified sheet and column.

        Args:
            gid: The sheet GID containing coupon data.
            column_name: The column header name for coupons.

        Returns:
            List of coupon codes.
        """
        return self.get_column_values_by_gid_and_name(
            gid=gid,
            column_name=column_name,
            skip_header=True,
        )
