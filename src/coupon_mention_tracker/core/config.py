"""Configuration settings for Coupon Mention Tracker."""

from functools import lru_cache

from pydantic import Field, computed_field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

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
    def normalize_database_url(cls, database_url: str) -> str:
        """Normalize database URL using SQLAlchemy's URL parser.

        Handles both raw and URL-encoded passwords correctly by parsing
        and re-encoding using SQLAlchemy's battle-tested implementation.
        """
        if isinstance(database_url, str):
            return make_url(database_url).render_as_string(hide_password=False)
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
