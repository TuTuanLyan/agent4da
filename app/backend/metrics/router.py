"""/metrics router - read-only dashboard KPIs from the Gold summaries via Trino.

Endpoints (auth required):
  GET /metrics/overview?as_of_date=YYYY-MM-DD
  GET /metrics/revenue?start_date=&end_date=
  GET /metrics/brands?start_date=&end_date=&limit=
  GET /metrics/categories?start_date=&end_date=&limit=
  GET /metrics/products?start_date=&end_date=&limit=

Contract follows docs/DASHBOARD_METRICS.md. If Trino is unreachable the endpoint
returns 503 with a clean message; the dashboard renders an empty/error state
rather than crashing.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query

from auth.deps import current_user
from db.models import User
from metrics import queries
from metrics.schemas import (
    BrandRow,
    CategoryRow,
    MetricsMeta,
    OverviewResponse,
    ProductRow,
    RevenuePoint,
)
from trino_client import execute_query_to_dicts


log = structlog.get_logger("metrics.router")
router = APIRouter(prefix="/metrics", tags=["metrics"])

DEFAULT_LIMIT = 10
MAX_LIMIT = 100


# --- coercion helpers (Trino returns Decimal/date/None) ---------------------

def _f(v: Any) -> Optional[float]:
    return None if v is None else float(v)


def _i(v: Any) -> Optional[int]:
    return None if v is None else int(v)


def _s(v: Any) -> Optional[str]:
    return None if v is None else str(v)


def _validate_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=422, detail="Date must be ISO format YYYY-MM-DD.")
    return value


def _clamp_limit(limit: int) -> int:
    return max(1, min(int(limit), MAX_LIMIT))


def _run(sql: str, params: List[Any]) -> List[Dict[str, Any]]:
    try:
        return execute_query_to_dicts(sql, params or None)
    except Exception as exc:  # noqa: BLE001
        log.warning("metrics.query_failed", error=str(exc))
        raise HTTPException(
            status_code=503,
            detail="Metrics source unavailable. Trino may be down or Gold tables are missing.",
        )


# --- endpoints --------------------------------------------------------------


@router.get("/overview", response_model=OverviewResponse)
def overview(
    as_of_date: Optional[str] = Query(None),
    user: User = Depends(current_user),
) -> OverviewResponse:
    as_of = _validate_date(as_of_date)
    sql, params = queries.overview_query(as_of)
    rows = _run(sql, params)
    meta = MetricsMeta(as_of_date=as_of)
    if not rows:
        return OverviewResponse(meta=meta)
    r = rows[0]
    return OverviewResponse(
        event_date=_s(r.get("event_date")),
        today_revenue=_f(r.get("today_revenue")),
        today_events=_i(r.get("today_events")),
        today_purchases=_i(r.get("today_purchases")),
        today_conversion_rate=_f(r.get("today_conversion_rate")),
        mtd_events=_i(r.get("mtd_events")),
        mtd_revenue=_f(r.get("mtd_revenue")),
        top_brand_mtd=_s(r.get("top_brand_mtd")),
        top_brand_mtd_revenue=_f(r.get("top_brand_mtd_revenue")),
        meta=meta,
    )


@router.get("/revenue", response_model=List[RevenuePoint])
def revenue(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    user: User = Depends(current_user),
) -> List[RevenuePoint]:
    start, end = _validate_date(start_date), _validate_date(end_date)
    sql, params = queries.revenue_query(start, end)
    rows = _run(sql, params)
    return [
        RevenuePoint(
            event_date=str(r.get("event_date")),
            total_revenue=_f(r.get("total_revenue")) or 0.0,
            total_events=_i(r.get("total_events")) or 0,
            total_purchases=_i(r.get("total_purchases")) or 0,
            conversion_rate=_f(r.get("conversion_rate")) or 0.0,
            cart_to_purchase_rate=_f(r.get("cart_to_purchase_rate")) or 0.0,
        )
        for r in rows
    ]


@router.get("/brands", response_model=List[BrandRow])
def brands(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    user: User = Depends(current_user),
) -> List[BrandRow]:
    start, end = _validate_date(start_date), _validate_date(end_date)
    sql, params = queries.brands_query(start, end, _clamp_limit(limit))
    rows = _run(sql, params)
    return [
        BrandRow(
            brand=_s(r.get("brand")) or "unknown",
            revenue=_f(r.get("revenue")) or 0.0,
            views=_i(r.get("views")) or 0,
            carts=_i(r.get("carts")) or 0,
            purchases=_i(r.get("purchases")) or 0,
            conversion_rate=_f(r.get("conversion_rate")) or 0.0,
        )
        for r in rows
    ]


@router.get("/categories", response_model=List[CategoryRow])
def categories(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    user: User = Depends(current_user),
) -> List[CategoryRow]:
    start, end = _validate_date(start_date), _validate_date(end_date)
    sql, params = queries.categories_query(start, end, _clamp_limit(limit))
    rows = _run(sql, params)
    return [
        CategoryRow(
            category_l1=_s(r.get("category_l1")) or "unknown",
            category_l2=_s(r.get("category_l2")) or "unknown",
            category_l3=_s(r.get("category_l3")) or "unknown",
            total_events=_i(r.get("total_events")) or 0,
            views=_i(r.get("views")) or 0,
            carts=_i(r.get("carts")) or 0,
            purchases=_i(r.get("purchases")) or 0,
            revenue=_f(r.get("revenue")) or 0.0,
            conversion_rate=_f(r.get("conversion_rate")) or 0.0,
            cart_to_purchase_rate=_f(r.get("cart_to_purchase_rate")) or 0.0,
        )
        for r in rows
    ]


@router.get("/products", response_model=List[ProductRow])
def products(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    user: User = Depends(current_user),
) -> List[ProductRow]:
    start, end = _validate_date(start_date), _validate_date(end_date)
    sql, params = queries.products_query(start, end, _clamp_limit(limit))
    rows = _run(sql, params)
    return [
        ProductRow(
            product_id=_s(r.get("product_id")),
            brand=_s(r.get("brand")) or "unknown",
            category_l1=_s(r.get("category_l1")) or "unknown",
            category_l2=_s(r.get("category_l2")) or "unknown",
            category_l3=_s(r.get("category_l3")) or "unknown",
            revenue=_f(r.get("revenue")) or 0.0,
            views=_i(r.get("views")) or 0,
            carts=_i(r.get("carts")) or 0,
            purchases=_i(r.get("purchases")) or 0,
            conversion_rate=_f(r.get("conversion_rate")) or 0.0,
        )
        for r in rows
    ]
