"""Response models for the dashboard metrics endpoints.

Shapes follow the sample responses in docs/DASHBOARD_METRICS.md.
"""

from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel


class MetricsMeta(BaseModel):
    as_of_date: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    source: str = "trino.iceberg.gold"


class OverviewResponse(BaseModel):
    event_date: Optional[str] = None
    today_revenue: Optional[float] = None
    today_events: Optional[int] = None
    today_purchases: Optional[int] = None
    today_conversion_rate: Optional[float] = None
    mtd_events: Optional[int] = None
    mtd_revenue: Optional[float] = None
    top_brand_mtd: Optional[str] = None
    top_brand_mtd_revenue: Optional[float] = None
    meta: MetricsMeta = MetricsMeta()


class RevenuePoint(BaseModel):
    event_date: str
    total_revenue: float = 0.0
    total_events: int = 0
    total_purchases: int = 0
    conversion_rate: float = 0.0
    cart_to_purchase_rate: float = 0.0


class BrandRow(BaseModel):
    brand: str = "unknown"
    revenue: float = 0.0
    views: int = 0
    carts: int = 0
    purchases: int = 0
    conversion_rate: float = 0.0


class CategoryRow(BaseModel):
    category_l1: str = "unknown"
    category_l2: str = "unknown"
    category_l3: str = "unknown"
    total_events: int = 0
    views: int = 0
    carts: int = 0
    purchases: int = 0
    revenue: float = 0.0
    conversion_rate: float = 0.0
    cart_to_purchase_rate: float = 0.0


class ProductRow(BaseModel):
    product_id: Optional[str] = None
    brand: str = "unknown"
    category_l1: str = "unknown"
    category_l2: str = "unknown"
    category_l3: str = "unknown"
    revenue: float = 0.0
    views: int = 0
    carts: int = 0
    purchases: int = 0
    conversion_rate: float = 0.0
