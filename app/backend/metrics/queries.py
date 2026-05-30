"""Canonical dashboard SQL against the Gold summaries (via Trino).

Mirrors docs/DASHBOARD_METRICS.md. The `:param` placeholders from the spec are
rendered as positional `?` binds for the trino-python-client; optional date
filters are built conditionally so we never bind a parameter twice. `limit` is
clamped to an int by the router and interpolated as a literal (safe).

All tables are fully qualified `iceberg.gold.*`, matching quickstats.
"""

from __future__ import annotations

from typing import Any, List, Optional, Tuple


# KPI overview. Binds [as_of_date | None]; COALESCE falls back to max(event_date).
OVERVIEW_SQL = """
WITH selected_day AS (
    SELECT COALESCE(
        CAST(? AS DATE),
        (SELECT max(event_date) FROM iceberg.gold.daily_event_summary)
    ) AS event_date
),
current_day AS (
    SELECT event_date, total_revenue, total_events, total_purchases, conversion_rate
    FROM iceberg.gold.daily_event_summary
    WHERE event_date = (SELECT event_date FROM selected_day)
),
month_to_date AS (
    SELECT sum(total_events) AS mtd_events, sum(total_revenue) AS mtd_revenue
    FROM iceberg.gold.daily_event_summary
    WHERE event_date >= date_trunc('month', (SELECT event_date FROM selected_day))
      AND event_date <= (SELECT event_date FROM selected_day)
),
top_brand AS (
    SELECT brand, sum(revenue) AS revenue, sum(purchase_count) AS purchases
    FROM iceberg.gold.daily_brand_summary
    WHERE event_date >= date_trunc('month', (SELECT event_date FROM selected_day))
      AND event_date <= (SELECT event_date FROM selected_day)
    GROUP BY brand
    ORDER BY revenue DESC, purchases DESC, brand ASC
    LIMIT 1
)
SELECT
    current_day.event_date,
    current_day.total_revenue AS today_revenue,
    current_day.total_events AS today_events,
    current_day.total_purchases AS today_purchases,
    current_day.conversion_rate AS today_conversion_rate,
    month_to_date.mtd_events,
    month_to_date.mtd_revenue,
    top_brand.brand AS top_brand_mtd,
    top_brand.revenue AS top_brand_mtd_revenue
FROM current_day
CROSS JOIN month_to_date
LEFT JOIN top_brand ON true
""".strip()


def _date_filter(start_date: Optional[str], end_date: Optional[str]) -> Tuple[str, List[Any]]:
    clauses: List[str] = []
    params: List[Any] = []
    if start_date:
        clauses.append("event_date >= CAST(? AS DATE)")
        params.append(start_date)
    if end_date:
        clauses.append("event_date <= CAST(? AS DATE)")
        params.append(end_date)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


def overview_query(as_of_date: Optional[str]) -> Tuple[str, List[Any]]:
    return OVERVIEW_SQL, [as_of_date]


def revenue_query(start_date: Optional[str], end_date: Optional[str]) -> Tuple[str, List[Any]]:
    where, params = _date_filter(start_date, end_date)
    sql = f"""
SELECT
    event_date,
    total_revenue,
    total_events,
    total_purchases,
    conversion_rate,
    cart_to_purchase_rate
FROM iceberg.gold.daily_event_summary
{where}
ORDER BY event_date ASC
""".strip()
    return sql, params


def brands_query(start_date: Optional[str], end_date: Optional[str], limit: int) -> Tuple[str, List[Any]]:
    where, params = _date_filter(start_date, end_date)
    sql = f"""
SELECT
    COALESCE(brand, 'unknown') AS brand,
    sum(revenue) AS revenue,
    sum(view_count) AS views,
    sum(cart_count) AS carts,
    sum(purchase_count) AS purchases,
    CASE
        WHEN sum(view_count) = 0 THEN 0.0
        ELSE CAST(sum(purchase_count) AS DOUBLE) / CAST(sum(view_count) AS DOUBLE)
    END AS conversion_rate
FROM iceberg.gold.daily_brand_summary
{where}
GROUP BY COALESCE(brand, 'unknown')
ORDER BY revenue DESC, purchases DESC, brand ASC
LIMIT {int(limit)}
""".strip()
    return sql, params


def categories_query(start_date: Optional[str], end_date: Optional[str], limit: int) -> Tuple[str, List[Any]]:
    where, params = _date_filter(start_date, end_date)
    sql = f"""
SELECT
    COALESCE(category_l1, 'unknown') AS category_l1,
    COALESCE(category_l2, 'unknown') AS category_l2,
    COALESCE(category_l3, 'unknown') AS category_l3,
    sum(total_events) AS total_events,
    sum(view_count) AS views,
    sum(cart_count) AS carts,
    sum(purchase_count) AS purchases,
    sum(revenue) AS revenue,
    CASE
        WHEN sum(view_count) = 0 THEN 0.0
        ELSE CAST(sum(purchase_count) AS DOUBLE) / CAST(sum(view_count) AS DOUBLE)
    END AS conversion_rate,
    CASE
        WHEN sum(cart_count) = 0 THEN 0.0
        ELSE CAST(sum(purchase_count) AS DOUBLE) / CAST(sum(cart_count) AS DOUBLE)
    END AS cart_to_purchase_rate
FROM iceberg.gold.daily_category_summary
{where}
GROUP BY
    COALESCE(category_l1, 'unknown'),
    COALESCE(category_l2, 'unknown'),
    COALESCE(category_l3, 'unknown')
ORDER BY revenue DESC, purchases DESC, category_l1 ASC, category_l2 ASC, category_l3 ASC
LIMIT {int(limit)}
""".strip()
    return sql, params


def products_query(start_date: Optional[str], end_date: Optional[str], limit: int) -> Tuple[str, List[Any]]:
    where, params = _date_filter(start_date, end_date)
    sql = f"""
SELECT
    product_id,
    COALESCE(brand, 'unknown') AS brand,
    COALESCE(category_l1, 'unknown') AS category_l1,
    COALESCE(category_l2, 'unknown') AS category_l2,
    COALESCE(category_l3, 'unknown') AS category_l3,
    sum(revenue) AS revenue,
    sum(view_count) AS views,
    sum(cart_count) AS carts,
    sum(purchase_count) AS purchases,
    CASE
        WHEN sum(view_count) = 0 THEN 0.0
        ELSE CAST(sum(purchase_count) AS DOUBLE) / CAST(sum(view_count) AS DOUBLE)
    END AS conversion_rate
FROM iceberg.gold.daily_product_summary
{where}
GROUP BY
    product_id,
    COALESCE(brand, 'unknown'),
    COALESCE(category_l1, 'unknown'),
    COALESCE(category_l2, 'unknown'),
    COALESCE(category_l3, 'unknown')
ORDER BY revenue DESC, purchases DESC, product_id ASC
LIMIT {int(limit)}
""".strip()
    return sql, params
