"""Canned SQL for the QuickStats strip above the Ask input.

All against Gold summary tables so the queries stay cheap.
"""

from __future__ import annotations


TODAY_REVENUE = """
SELECT COALESCE(SUM(total_revenue), 0) AS today_revenue
FROM iceberg.gold.daily_event_summary
WHERE event_date = current_date
""".strip()


MTD_EVENTS = """
SELECT COALESCE(SUM(total_events), 0) AS mtd_events
FROM iceberg.gold.daily_event_summary
WHERE event_date >= date_trunc('month', current_date)
""".strip()


MTD_TOP_BRAND = """
SELECT brand, COALESCE(SUM(revenue), 0) AS revenue
FROM iceberg.gold.daily_brand_summary
WHERE event_date >= date_trunc('month', current_date)
GROUP BY brand
ORDER BY revenue DESC
LIMIT 1
""".strip()
