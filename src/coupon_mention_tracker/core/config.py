"""Configuration settings for Coupon Mention Tracker."""

from functools import lru_cache
from urllib.parse import quote_plus, urlparse

from pydantic import Field, computed_field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    @staticmethod
    def _encode_database_url(url: str) -> str:
        """URL-encode the password in a database URL if needed."""
        parsed = urlparse(url)
        if not parsed.password:
            return url

        encoded_password = quote_plus(parsed.password)
        if parsed.port:
            netloc = (
                f"{parsed.username}:{encoded_password}@"
                f"{parsed.hostname}:{parsed.port}"
            )
        else:
            netloc = f"{parsed.username}:{encoded_password}@{parsed.hostname}"

        return f"{parsed.scheme}://{netloc}{parsed.path}"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Database
    database_url: str = Field(
        ...,
        description="PostgreSQL connection URL (e.g., postgresql://user:pass@host:port/db)",
    )
    cloud_sql_instance_connection_name: str | None = Field(
        default=None,
        description=(
            "Cloud SQL instance connection name "
            "(e.g., project:region:instance). "
            "If set, uses Cloud SQL Connector instead of direct connection."
        ),
    )

    @field_validator("database_url", mode="before")
    @classmethod
    def encode_password_in_url(cls, database_url: str) -> str:
        """Automatically URL-encode special characters in the password."""
        if isinstance(database_url, str):
            return cls._encode_database_url(database_url)
        return database_url

    @computed_field  # type: ignore[prop-decorator]
    @property
    def database_url_str(self) -> str:
        """Return database URL as string."""
        return self.database_url

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
