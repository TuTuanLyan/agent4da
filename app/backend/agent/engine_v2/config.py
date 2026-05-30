"""Shared constants for the v2 engine.

The single source of truth for the Trino catalog/schema the agent is allowed
to read. This matches the live Trino catalog name (`iceberg`) and the rest of
the backend (metrics, quickstats, catalog browser).
"""

from __future__ import annotations

import os

# Trino catalog + Gold schema. The agent only ever reads Gold.
GOLD_CATALOG = "iceberg"
GOLD_SCHEMA = "gold"
GOLD_PREFIX = f"{GOLD_CATALOG}.{GOLD_SCHEMA}"

# Retry budget for the SQL self-correction loop.
MAX_SQL_RETRY_ATTEMPTS = int(os.getenv("MAX_SQL_RETRY_ATTEMPTS", "3"))

# Persisted/returned result caps (mirrors the plan).
MAX_RESULT_ROWS = 10_000
MAX_CHART_ROWS = 20
MAX_LLM_ROWS = 10

# The canonical Gold tables the agent may query.
GOLD_TABLES = [
    "daily_brand_summary",
    "daily_category_summary",
    "daily_event_summary",
    "daily_product_summary",
    "dim_product",
    "dim_session",
    "dim_time",
    "dim_user",
    "fact_events",
    "fact_sales",
]
