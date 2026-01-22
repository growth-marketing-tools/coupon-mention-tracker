"""Configuration settings for Coupon Mention Tracker."""

from functools import lru_cache
from urllib.parse import quote_plus

from pydantic import Field, SecretStr, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Database
    database_user: str = Field(
        ...,
        description="PostgreSQL username",
    )
    database_password: SecretStr = Field(
        ...,
        description="PostgreSQL password",
    )
    database_name: str = Field(
        ...,
        description="PostgreSQL database name",
    )
    database_host: str = Field(
        default="localhost",
        description="PostgreSQL host (used for local dev with proxy)",
    )
    database_port: int = Field(
        default=5432,
        description="PostgreSQL port",
    )
    cloud_sql_instance_connection_name: str | None = Field(
        default=None,
        description="Cloud SQL instance connection name (e.g., project:region:instance). If set, uses Cloud SQL Connector instead of direct connection.",
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
    def database_dsn(self) -> str:
        """Construct database DSN for asyncpg (local dev with proxy)."""
        encoded_password = quote_plus(self.database_password.get_secret_value())
        return (
            f"postgresql://{self.database_user}:{encoded_password}@"
            f"{self.database_host}:{self.database_port}/{self.database_name}"
        )


@lru_cache
def get_settings() -> Settings:
    """Get application settings singleton (cached)."""
    return Settings()
