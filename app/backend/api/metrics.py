from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Query

from .auth import current_user
from .integrations import trino_query

router = APIRouter(tags=["metrics"])

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def safe_date(value: Optional[str]) -> Optional[str]:
    if value and DATE_RE.match(value):
        return value
    return None


def date_filter(column: str, start_date: Optional[str], end_date: Optional[str]) -> str:
    clauses = []
    start = safe_date(start_date)
    end = safe_date(end_date)
    if start:
        clauses.append(f"{column} >= DATE '{start}'")
    if end:
        clauses.append(f"{column} <= DATE '{end}'")
    return ("WHERE " + " AND ".join(clauses)) if clauses else ""


def first(rows: list[dict], fallback: dict) -> dict:
    return rows[0] if rows else fallback


@router.get("/metrics/overview")
def overview(
    as_of_date: Optional[str] = None,
    _user: dict = Depends(current_user),
) -> dict:
    as_of = safe_date(as_of_date)
    upper = f"WHERE event_date <= DATE '{as_of}'" if as_of else ""
    latest = first(
        trino_query(
            f"""
            SELECT CAST(event_date AS varchar) AS event_date,
                   CAST(total_revenue AS double) AS today_revenue,
                   CAST(total_events AS bigint) AS today_events,
                   CAST(total_purchases AS bigint) AS today_purchases,
                   CAST(conversion_rate AS double) AS today_conversion_rate
            FROM daily_event_summary
            {upper}
            ORDER BY event_date DESC
            LIMIT 1
            """
        ),
        {
            "event_date": None,
            "today_revenue": None,
            "today_events": None,
            "today_purchases": None,
            "today_conversion_rate": None,
        },
    )
    if latest["event_date"]:
        month_start = latest["event_date"][:8] + "01"
        mtd = first(
            trino_query(
                f"""
                SELECT CAST(sum(total_events) AS bigint) AS mtd_events,
                       CAST(sum(total_revenue) AS double) AS mtd_revenue
                FROM daily_event_summary
                WHERE event_date BETWEEN DATE '{month_start}' AND DATE '{latest["event_date"]}'
                """
            ),
            {"mtd_events": None, "mtd_revenue": None},
        )
        top_brand = first(
            trino_query(
                f"""
                SELECT brand AS top_brand_mtd,
                       CAST(sum(revenue) AS double) AS top_brand_mtd_revenue
                FROM daily_brand_summary
                WHERE event_date BETWEEN DATE '{month_start}' AND DATE '{latest["event_date"]}'
                GROUP BY brand
                ORDER BY top_brand_mtd_revenue DESC
                LIMIT 1
                """
            ),
            {"top_brand_mtd": None, "top_brand_mtd_revenue": None},
        )
    else:
        mtd = {"mtd_events": None, "mtd_revenue": None}
        top_brand = {"top_brand_mtd": None, "top_brand_mtd_revenue": None}
    return {**latest, **mtd, **top_brand}


@router.get("/metrics/revenue")
def revenue(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    _user: dict = Depends(current_user),
) -> list[dict]:
    where = date_filter("event_date", start_date, end_date)
    return trino_query(
        f"""
        SELECT CAST(event_date AS varchar) AS event_date,
               CAST(total_revenue AS double) AS total_revenue,
               CAST(total_events AS bigint) AS total_events,
               CAST(total_purchases AS bigint) AS total_purchases,
               CAST(conversion_rate AS double) AS conversion_rate,
               CAST(cart_to_purchase_rate AS double) AS cart_to_purchase_rate
        FROM daily_event_summary
        {where}
        ORDER BY event_date
        LIMIT 366
        """
    )


@router.get("/metrics/brands")
def brands(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = Query(default=10, ge=1, le=100),
    _user: dict = Depends(current_user),
) -> list[dict]:
    where = date_filter("event_date", start_date, end_date)
    return trino_query(
        f"""
        SELECT COALESCE(brand, 'unknown') AS brand,
               CAST(sum(revenue) AS double) AS revenue,
               CAST(sum(view_count) AS bigint) AS views,
               CAST(sum(cart_count) AS bigint) AS carts,
               CAST(sum(purchase_count) AS bigint) AS purchases,
               CAST(CASE WHEN sum(view_count) = 0 THEN 0 ELSE sum(purchase_count) * 1.0 / sum(view_count) END AS double) AS conversion_rate
        FROM daily_brand_summary
        {where}
        GROUP BY brand
        ORDER BY revenue DESC
        LIMIT {limit}
        """
    )


@router.get("/metrics/categories")
def categories(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = Query(default=10, ge=1, le=100),
    _user: dict = Depends(current_user),
) -> list[dict]:
    where = date_filter("event_date", start_date, end_date)
    return trino_query(
        f"""
        SELECT COALESCE(category_l1, 'unknown') AS category_l1,
               COALESCE(category_l2, '') AS category_l2,
               COALESCE(category_l3, '') AS category_l3,
               CAST(sum(view_count + cart_count + purchase_count + remove_from_cart_count) AS bigint) AS total_events,
               CAST(sum(view_count) AS bigint) AS views,
               CAST(sum(cart_count) AS bigint) AS carts,
               CAST(sum(purchase_count) AS bigint) AS purchases,
               CAST(sum(revenue) AS double) AS revenue,
               CAST(CASE WHEN sum(view_count) = 0 THEN 0 ELSE sum(purchase_count) * 1.0 / sum(view_count) END AS double) AS conversion_rate,
               CAST(CASE WHEN sum(cart_count) = 0 THEN 0 ELSE sum(purchase_count) * 1.0 / sum(cart_count) END AS double) AS cart_to_purchase_rate
        FROM daily_category_summary
        {where}
        GROUP BY category_l1, category_l2, category_l3
        ORDER BY revenue DESC
        LIMIT {limit}
        """
    )


@router.get("/metrics/products")
def products(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = Query(default=10, ge=1, le=100),
    _user: dict = Depends(current_user),
) -> list[dict]:
    where = date_filter("event_date", start_date, end_date)
    return trino_query(
        f"""
        SELECT CAST(product_id AS varchar) AS product_id,
               COALESCE(brand, 'unknown') AS brand,
               COALESCE(category_l1, 'unknown') AS category_l1,
               COALESCE(category_l2, '') AS category_l2,
               COALESCE(category_l3, '') AS category_l3,
               CAST(sum(revenue) AS double) AS revenue,
               CAST(sum(view_count) AS bigint) AS views,
               CAST(sum(cart_count) AS bigint) AS carts,
               CAST(sum(purchase_count) AS bigint) AS purchases,
               CAST(CASE WHEN sum(view_count) = 0 THEN 0 ELSE sum(purchase_count) * 1.0 / sum(view_count) END AS double) AS conversion_rate
        FROM daily_product_summary
        {where}
        GROUP BY product_id, brand, category_l1, category_l2, category_l3
        ORDER BY revenue DESC
        LIMIT {limit}
        """
    )


@router.get("/quickstats")
def quickstats(_user: dict = Depends(current_user)) -> dict:
    ov = overview(_user=_user)
    return {
        "today_revenue": ov.get("today_revenue"),
        "mtd_events": ov.get("mtd_events"),
        "mtd_top_brand_name": ov.get("top_brand_mtd"),
        "mtd_top_brand_revenue": ov.get("top_brand_mtd_revenue"),
        "cached_at": datetime.now(timezone.utc).isoformat(),
        "source_status": "ok" if ov.get("event_date") else "unavailable",
    }

