"""Configuration settings for Coupon Mention Tracker."""

from functools import lru_cache

from pydantic import Field, PostgresDsn, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Database
    database_url: PostgresDsn = Field(
        ...,
        description="PostgreSQL connection string",
    )

    # Slack
    slack_webhook_url: str = Field(
        ...,
        description="Slack webhook URL for sending alerts",
    )
    slack_channel: str = Field(
        default="#growth-marketing-alerts",
        description="Default Slack channel for alerts",
    )

    # Google Sheets
    google_sheets_spreadsheet_id: str = Field(
        default="19gKGRN_FjplOpMUqFtGOrR53htFZR67mUlWrJDWT4UI",
        description="Google Sheets spreadsheet ID for coupon data",
    )
    google_sheets_coupon_gid: int = Field(
        default=0,
        description="Sheet GID containing coupon codes",
    )
    google_sheets_coupon_column: str = Field(
        default="Coupon",
        description="Column name containing coupon codes",
    )
    google_workspace_credentials: str = Field(
        ...,
        description="Google Workspace service account credentials JSON",
    )

    # Report settings
    report_lookback_days: int = Field(
        default=7,
        description="Number of days to look back for weekly report",
    )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def database_url_str(self) -> str:
        """Get database URL as string for asyncpg."""
        return str(self.database_url)


@lru_cache
def get_settings() -> Settings:
    """Get application settings singleton (cached)."""
    return Settings()
