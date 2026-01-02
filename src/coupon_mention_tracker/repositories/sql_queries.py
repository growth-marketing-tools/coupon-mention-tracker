"""SQL queries for the AI Overview repository."""

from typing import Any


def build_get_prompts_query(
    product: str | None = None,
    location: str | None = None,
    status: str = "active",
) -> tuple[str, list[Any]]:
    """Build the SQL query and parameters for fetching prompts.

    Args:
        product: Filter by product (e.g., 'nordvpn', 'nordpass').
        location: Filter by location/country code.
        status: Filter by status (default: 'active').

    Returns:
        Tuple containing the SQL query string and the list of parameters.
    """
    query = """
        SELECT id, prompt_text, primary_product, location, status, tags,
               created_at
        FROM marketing_hub.ai_overviews_prompts
        WHERE 1=1
    """
    params: list[Any] = []
    param_idx = 1

    if status:
        query += f" AND status = ${param_idx}"
        params.append(status)
        param_idx += 1

    if product:
        query += f" AND primary_product = ${param_idx}"
        params.append(product)
        param_idx += 1

    if location:
        query += f" AND location = ${param_idx}"
        params.append(location)

    query += " ORDER BY created_at DESC"

    return query, params


GET_RESULTS_FOR_PERIOD_QUERY = """
    SELECT
        p.id as prompt_id,
        p.prompt_text,
        p.primary_product,
        p.location,
        p.status,
        p.tags,
        p.created_at as prompt_created_at,
        r.id as result_id,
        r.provider,
        r.scraped_date,
        r.scraped_at,
        r.response_text,
        r.sources,
        r.ahrefs_volume,
        r.sentiment_label
    FROM marketing_hub.ai_overviews_results r
    JOIN marketing_hub.ai_overviews_prompts p ON r.prompt_id = p.id
    WHERE r.scraped_date BETWEEN $1 AND $2
      AND r.provider = $3
      AND r.response_text IS NOT NULL
    ORDER BY r.scraped_date DESC, p.prompt_text
"""
