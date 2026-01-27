"""SQLAlchemy table definitions for type-safe database queries.

This module defines all database tables using SQLAlchemy Core for type safety,
autocomplete, and better query composition.
"""

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, TSVECTOR, UUID


marketing_hub_metadata = MetaData(schema="marketing_hub")

ai_overviews_prompts = Table(
    "ai_overviews_prompts",
    marketing_hub_metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("prompt_text", Text, nullable=False),
    Column("primary_product", String, nullable=False),
    Column("location", String),
    Column("status", String),
    Column("tags", ARRAY(Text)),
    Column("created_at", DateTime(timezone=True)),
    Column("updated_at", DateTime(timezone=True)),
)

ai_overviews_results = Table(
    "ai_overviews_results",
    marketing_hub_metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("prompt_id", UUID(as_uuid=True), nullable=False),
    Column("provider", String, nullable=False),
    Column("scraped_date", Date, nullable=False),
    Column("scraped_at", DateTime(timezone=True)),
    Column("response_text", Text),
    Column("sources", JSONB),
    Column("raw_response", JSONB),
    Column("ahrefs_volume", Integer),
    Column("sentiment_score", Integer),
    Column("sentiment_label", Text),
    Column("created_at", DateTime(timezone=True)),
)

ai_overviews_sources = Table(
    "ai_overviews_sources",
    marketing_hub_metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("source_url", Text, nullable=False),
    Column("source_domain", Text, nullable=False),
    Column("normalized_url", Text),
    Column("source_type", Text),
    Column("url_type", Text),
    Column("source_html_content", Text),
    Column("page_title", Text),
    Column("page_description", Text),
    Column("first_seen_at", DateTime, nullable=False),
    Column("last_seen_at", DateTime, nullable=False),
    Column("last_updated_at", DateTime, nullable=False),
    Column("scraped_at", DateTime),
    Column("scrape_status", Text),
    Column("scrape_error", Text),
    Column("metadata", JSONB),
    Column("created_at", DateTime, nullable=False),
    Column("updated_at", DateTime, nullable=False),
    Column("primary_product", Text),
    Column("search_vector", TSVECTOR),
)

ai_overviews_competitors = Table(
    "ai_overviews_competitors",
    marketing_hub_metadata,
    Column("id", Integer, primary_key=True),
    Column("competitor_name", String, nullable=False),
    Column("competitor_domain", String),
    Column("product", String, nullable=False),
    Column("created_at", DateTime(timezone=True)),
    Column("updated_at", DateTime(timezone=True)),
    Column("display_name", Text),
    Column("is_suggested", Boolean),
    Column("mention_count", Integer),
    Column("color", String),
)

ai_overviews_tags = Table(
    "ai_overviews_tags",
    marketing_hub_metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("name", Text, nullable=False),
    Column("slug", Text, nullable=False),
    Column("description", Text),
    Column("color", Text),
    Column("is_active", Boolean, nullable=False),
    Column("created_by", UUID(as_uuid=True)),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

# Looker Schema
looker_metadata = MetaData(schema="looker")

coupon_tracking_history = Table(
    "coupon_tracking_history",
    looker_metadata,
    Column(
        "id",
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    ),
    Column("keyword", Text, nullable=False),
    Column("location", String),
    Column("primary_product", String),
    Column("has_ai_overview", Boolean, nullable=False),
    Column("ai_overview_result_id", UUID(as_uuid=True)),
    Column("tracked_coupon_present", Boolean, nullable=False),
    Column("detected_coupon_code", String),
    Column("is_valid_coupon", Boolean),
    Column("match_context", Text),
    Column("scraped_date", Date, nullable=False),
    Column(
        "source_mention_count",
        Integer,
        server_default=text("0"),
        nullable=False,
    ),
    Column("source_urls_with_mentions", ARRAY(Text)),
    Column(
        "source_mention_unavailable",
        Boolean,
        server_default=text("false"),
        nullable=False,
    ),
    Column(
        "created_at",
        DateTime(timezone=True),
        server_default=text("now()"),
        nullable=False,
    ),
)

# Public Schema (SEO Team Content)
public_metadata = MetaData(schema="public")

seo_team_content = Table(
    "seo_team_content",
    public_metadata,
    Column("id", Integer),
    Column("domain", String),
    Column("dr", Integer),
    Column("traffic", Integer),
    Column("country", String),
    Column("target_url", Text),
    Column("main_kw", Text),
    Column("title", Text),
    Column("word_count", Integer),
    Column("requested_by", String),
    Column("free_placement", String),
    Column("due_date", Date),
    Column("freelancers_name", String),
    Column("price", Numeric),
    Column("status", String),
    Column("unique_identifier", String),
)
