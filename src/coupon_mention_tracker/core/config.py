"""Configuration settings for Coupon Mention Tracker."""

from functools import lru_cache
from urllib.parse import quote, unquote, urlsplit, urlunsplit

from pydantic import Field, computed_field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    database_url: str = Field(
        ...,
        description="PostgreSQL connection URL (e.g., postgresql://user:pass@host:port/db)",
    )

    @field_validator("database_url", mode="before")
    @classmethod
    def normalize_database_url(cls, database_url: str) -> str:
        """Ensure database URL is a string."""
        return str(database_url)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def database_url_str(self) -> str:
        """Return database URL with user credentials URL-encoded."""
        parts = urlsplit(self.database_url)
        if parts.username is None and parts.password is None:
            return self.database_url

        username = quote(unquote(parts.username or ""), safe="")
        password = quote(unquote(parts.password or ""), safe="")
        hostname = parts.hostname or ""
        if ":" in hostname and not hostname.startswith("["):
            hostname = f"[{hostname}]"
        port = f":{parts.port}" if parts.port is not None else ""
        userinfo = username
        if parts.password is not None:
            userinfo = f"{username}:{password}"
        netloc = f"{userinfo}@{hostname}{port}"
        return urlunsplit(
            (parts.scheme, netloc, parts.path, parts.query, parts.fragment)
        )

    slack_webhook_url: str = Field(
        ...,
        description="Slack webhook URL for sending alerts",
    )
    slack_channel: str = Field(
        default="#growth-marketing-alerts",
        description="Default Slack channel for alerts",
    )

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

    report_lookback_days: int = Field(
        default=7,
        description="Number of days to look back for weekly report",
    )


@lru_cache
def get_settings() -> Settings:
    """Get application settings singleton (cached)."""
    return Settings()
