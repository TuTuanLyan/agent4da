"""Dashboard metrics service backed by Trino Gold tables.

This module is intentionally framework-free so it can be reused by a future
FastAPI/Flask endpoint, CLI smoke checks, or the agent UI backend.
"""

from datetime import date

from services.trino_service import execute_query_to_dicts, get_trino_connection


DEFAULT_CATALOG = "iceberg"
DEFAULT_SCHEMA = "gold"
DEFAULT_LIMIT = 10
MAX_LIMIT = 100


def _date_literal(value, name):
    try:
        parsed = date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an ISO date in YYYY-MM-DD format.") from exc

    return f"DATE '{parsed.isoformat()}'"


def _optional_date_filter(column_name, start_date=None, end_date=None):
    filters = []
    if start_date:
        filters.append(f"{column_name} >= {_date_literal(start_date, 'start_date')}")
    if end_date:
        filters.append(f"{column_name} <= {_date_literal(end_date, 'end_date')}")
    return " AND ".join(filters) if filters else "1 = 1"


def _bounded_limit(limit):
    try:
        value = int(limit)
    except (TypeError, ValueError) as exc:
        raise ValueError("limit must be an integer.") from exc

    if value < 1:
        raise ValueError("limit must be greater than 0.")
    return min(value, MAX_LIMIT)


def _connection(connection=None):
    return connection or get_trino_connection(catalog=DEFAULT_CATALOG, schema=DEFAULT_SCHEMA)


def get_dashboard_overview(connection=None, as_of_date=None):
    """Return headline KPI cards for the dashboard.

    If as_of_date is omitted, the latest event_date in daily_event_summary is used.
    """

    date_filter = (
        f"event_date = {_date_literal(as_of_date, 'as_of_date')}"
        if as_of_date
        else "event_date = (SELECT max(event_date) FROM daily_event_summary)"
    )
    query = f"""
        WITH current_day AS (
            SELECT
                event_date,
                total_revenue,
                total_events,
                total_purchases,
                conversion_rate
            FROM daily_event_summary
            WHERE {date_filter}
        ),
        month_to_date AS (
            SELECT
                date_trunc('month', event_date) AS metric_month,
                sum(total_events) AS mtd_events,
                sum(total_revenue) AS mtd_revenue
            FROM daily_event_summary
            WHERE event_date <= (SELECT max(event_date) FROM current_day)
              AND date_trunc('month', event_date) = (
                  SELECT date_trunc('month', max(event_date)) FROM current_day
              )
            GROUP BY 1
        ),
        top_brand AS (
            SELECT brand, sum(revenue) AS revenue
            FROM daily_brand_summary
            WHERE event_date <= (SELECT max(event_date) FROM current_day)
              AND date_trunc('month', event_date) = (
                  SELECT date_trunc('month', max(event_date)) FROM current_day
              )
            GROUP BY brand
            ORDER BY revenue DESC, brand ASC
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
    """
    rows = execute_query_to_dicts(_connection(connection), query, raise_on_error=True)
    return rows[0] if rows else {}


def get_revenue_timeseries(connection=None, start_date=None, end_date=None):
    where_clause = _optional_date_filter("event_date", start_date, end_date)
    query = f"""
        SELECT
            event_date,
            total_revenue,
            total_events,
            total_purchases,
            conversion_rate,
            cart_to_purchase_rate
        FROM daily_event_summary
        WHERE {where_clause}
        ORDER BY event_date ASC
    """
    return execute_query_to_dicts(_connection(connection), query, raise_on_error=True)


def get_top_brands(connection=None, start_date=None, end_date=None, limit=DEFAULT_LIMIT):
    where_clause = _optional_date_filter("event_date", start_date, end_date)
    limit_value = _bounded_limit(limit)
    query = f"""
        SELECT
            brand,
            sum(revenue) AS revenue,
            sum(view_count) AS views,
            sum(cart_count) AS carts,
            sum(purchase_count) AS purchases,
            CASE
                WHEN sum(view_count) = 0 THEN 0.0
                ELSE cast(sum(purchase_count) AS double) / cast(sum(view_count) AS double)
            END AS conversion_rate
        FROM daily_brand_summary
        WHERE {where_clause}
        GROUP BY brand
        ORDER BY revenue DESC, purchases DESC, brand ASC
        LIMIT {limit_value}
    """
    return execute_query_to_dicts(_connection(connection), query, raise_on_error=True)


def get_category_conversion(connection=None, start_date=None, end_date=None, limit=DEFAULT_LIMIT):
    where_clause = _optional_date_filter("event_date", start_date, end_date)
    limit_value = _bounded_limit(limit)
    query = f"""
        SELECT
            category_l1,
            category_l2,
            category_l3,
            sum(total_events) AS total_events,
            sum(view_count) AS views,
            sum(cart_count) AS carts,
            sum(purchase_count) AS purchases,
            sum(revenue) AS revenue,
            CASE
                WHEN sum(view_count) = 0 THEN 0.0
                ELSE cast(sum(purchase_count) AS double) / cast(sum(view_count) AS double)
            END AS conversion_rate,
            CASE
                WHEN sum(cart_count) = 0 THEN 0.0
                ELSE cast(sum(purchase_count) AS double) / cast(sum(cart_count) AS double)
            END AS cart_to_purchase_rate
        FROM daily_category_summary
        WHERE {where_clause}
        GROUP BY category_l1, category_l2, category_l3
        ORDER BY revenue DESC, purchases DESC, category_l1 ASC, category_l2 ASC, category_l3 ASC
        LIMIT {limit_value}
    """
    return execute_query_to_dicts(_connection(connection), query, raise_on_error=True)


def get_product_leaderboard(connection=None, start_date=None, end_date=None, limit=DEFAULT_LIMIT):
    where_clause = _optional_date_filter("event_date", start_date, end_date)
    limit_value = _bounded_limit(limit)
    query = f"""
        SELECT
            product_id,
            brand,
            category_l1,
            category_l2,
            category_l3,
            sum(revenue) AS revenue,
            sum(view_count) AS views,
            sum(cart_count) AS carts,
            sum(purchase_count) AS purchases,
            CASE
                WHEN sum(view_count) = 0 THEN 0.0
                ELSE cast(sum(purchase_count) AS double) / cast(sum(view_count) AS double)
            END AS conversion_rate
        FROM daily_product_summary
        WHERE {where_clause}
        GROUP BY product_id, brand, category_l1, category_l2, category_l3
        ORDER BY revenue DESC, purchases DESC, product_id ASC
        LIMIT {limit_value}
    """
    return execute_query_to_dicts(_connection(connection), query, raise_on_error=True)
