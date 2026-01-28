"""Raw SQL queries for the repositories."""

GET_PROMPTS_BASE = """
    SELECT id, prompt_text, primary_product, location, status, tags, created_at
    FROM marketing_hub.ai_overviews_prompts
    WHERE status = $1
"""

GET_RESULTS_FOR_PERIOD = """
    SELECT
        p.id as prompt_id, p.prompt_text, p.primary_product, p.location,
        p.status, p.tags, p.created_at as prompt_created_at,
        r.id as result_id, r.provider, r.scraped_date, r.scraped_at,
        r.response_text, r.sources, r.ahrefs_volume, r.sentiment_label
    FROM marketing_hub.ai_overviews_results r
    JOIN marketing_hub.ai_overviews_prompts p ON r.prompt_id = p.id
    WHERE r.scraped_date BETWEEN $1 AND $2
      AND r.provider = $3
      AND r.response_text IS NOT NULL
"""

GET_SOURCES_FOR_RESULTS = """
    SELECT DISTINCT ON (source_item->>'url', r.id)
        s.id,
        source_item->>'url' as source_url,
        s.source_domain,
        s.source_html_content,
        s.page_title,
        s.scraped_at,
        s.scrape_status,
        r.id as result_id
    FROM marketing_hub.ai_overviews_results r
    CROSS JOIN LATERAL jsonb_array_elements(
        CASE
            WHEN jsonb_typeof(r.sources) = 'array' THEN r.sources
            ELSE '[]'::jsonb
        END
    ) AS source_item
    JOIN marketing_hub.ai_overviews_sources s
        ON s.source_url = SPLIT_PART(source_item->>'url', '#', 1)
    WHERE r.id = ANY($1::uuid[])
      AND s.source_html_content IS NOT NULL
      AND s.scrape_status = 'success'
    ORDER BY source_item->>'url', r.id, s.scraped_at DESC
"""

UPSERT_TRACKING_HISTORY = """
    INSERT INTO looker.coupon_tracking_history
    (
        keyword,
        location,
        primary_product,
        has_ai_overview,
        ai_overview_result_id,
        tracked_coupon_present,
        detected_coupon_code,
        is_valid_coupon,
        match_context,
        scraped_date,
        source_mention_count,
        source_urls_with_mentions,
        source_mention_unavailable
    )
    VALUES (
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8,
        $9,
        $10,
        $11,
        $12,
        $13
    )
    ON CONFLICT (keyword, location, scraped_date)
    DO UPDATE SET
        has_ai_overview = EXCLUDED.has_ai_overview,
        ai_overview_result_id = EXCLUDED.ai_overview_result_id,
        tracked_coupon_present = EXCLUDED.tracked_coupon_present,
        detected_coupon_code = EXCLUDED.detected_coupon_code,
        is_valid_coupon = EXCLUDED.is_valid_coupon,
        match_context = EXCLUDED.match_context,
        source_mention_count = EXCLUDED.source_mention_count,
        source_urls_with_mentions = EXCLUDED.source_urls_with_mentions,
        source_mention_unavailable = EXCLUDED.source_mention_unavailable
"""
