"""/quickstats router - small KPI strip for the Ask screen.

Three numbers from the Gold daily summaries:
- today_revenue (sum total_revenue for current_date)
- mtd_events   (sum total_events for the current month)
- mtd_top_brand (single brand with highest revenue MTD)

60-second TTL cache. If Trino is unreachable we degrade gracefully:
the field becomes `null` with a status flag, never an HTTP 5xx, so the
UI keeps loading the rest of the Ask screen.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Optional

import structlog
from fastapi import APIRouter, Depends
from pydantic import BaseModel

from auth.deps import current_user
from db.models import User
from quickstats.queries import MTD_EVENTS, MTD_TOP_BRAND, TODAY_REVENUE
from trino_client import TTLCache, execute_query_to_dicts


log = structlog.get_logger("quickstats.router")
router = APIRouter(prefix="/quickstats", tags=["quickstats"])


class QuickStatsResponse(BaseModel):
    today_revenue: Optional[float] = None
    mtd_events: Optional[int] = None
    mtd_top_brand_name: Optional[str] = None
    mtd_top_brand_revenue: Optional[float] = None
    cached_at: datetime
    source_status: str  # 'ok' | 'partial' | 'unavailable'


_CACHE = TTLCache(ttl_seconds=60)


def _safe_scalar(sql: str, field: str):
    try:
        rows = execute_query_to_dicts(sql)
        if not rows:
            return None
        return rows[0].get(field)
    except Exception as exc:
        log.warning("quickstats.query_failed", field=field, error=str(exc))
        return _SENTINEL_ERROR


def _safe_top_brand():
    try:
        rows = execute_query_to_dicts(MTD_TOP_BRAND)
        if not rows:
            return None, None
        row = rows[0]
        return row.get("brand"), row.get("revenue")
    except Exception as exc:
        log.warning("quickstats.top_brand_failed", error=str(exc))
        return _SENTINEL_ERROR, _SENTINEL_ERROR


class _Sentinel:
    pass


_SENTINEL_ERROR = _Sentinel()


def _build() -> QuickStatsResponse:
    today = _safe_scalar(TODAY_REVENUE, "today_revenue")
    mtd = _safe_scalar(MTD_EVENTS, "mtd_events")
    brand, brand_rev = _safe_top_brand()

    errors = sum(
        1
        for v in (today, mtd, brand)
        if isinstance(v, _Sentinel)
    )
    status = "ok"
    if errors == 3:
        status = "unavailable"
    elif errors > 0:
        status = "partial"

    def clean(v):
        return None if isinstance(v, _Sentinel) else v

    today_clean = clean(today)
    mtd_clean = clean(mtd)
    brand_clean = clean(brand)
    brand_rev_clean = clean(brand_rev)

    return QuickStatsResponse(
        today_revenue=float(today_clean) if today_clean is not None else None,
        mtd_events=int(mtd_clean) if mtd_clean is not None else None,
        mtd_top_brand_name=str(brand_clean) if brand_clean is not None else None,
        mtd_top_brand_revenue=float(brand_rev_clean) if brand_rev_clean is not None else None,
        cached_at=datetime.now(timezone.utc),
        source_status=status,
    )


@router.get("", response_model=QuickStatsResponse)
def get_quickstats(user: User = Depends(current_user)) -> QuickStatsResponse:
    return _CACHE.get(_build)
