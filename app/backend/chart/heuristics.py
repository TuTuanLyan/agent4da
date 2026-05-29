"""Rule-based chart picker.

The frontend mirrors these rules in lib/chart-pick.ts so the UI can change
its mind without a server round-trip when the user toggles 'Auto'.

Rules (first match wins):
  1. 1 date/timestamp col + 1 numeric col      -> line, x=date, y=numeric
  2. 1 date col + multiple numeric             -> stacked line
  3. 1 categorical (<=20 distinct) + 1 numeric -> bar, sorted desc
  4. 1 categorical (<=8 distinct) + 1 numeric named like share/pct/rate
                                                -> pie
  5. 2 numeric                                  -> scatter
  6. otherwise                                  -> table only
"""

from __future__ import annotations

import datetime as _dt
import decimal
import math
import re
from typing import Any, Dict, List, Optional, Tuple

NUMERIC_TYPES = (int, float, decimal.Decimal)
DATE_LIKE_TYPES = (_dt.date, _dt.datetime)

DATE_COLUMN_HINTS = re.compile(
    r"(_date$|^date|_at$|^ts$|_ts$|_time$|^time$)", flags=re.IGNORECASE
)
PCT_COLUMN_HINTS = re.compile(
    r"(share|ratio|rate|pct|percent)", flags=re.IGNORECASE
)


def _classify_column(name: str, samples: List[Any]) -> str:
    """Return one of: 'date', 'numeric', 'categorical', 'unknown'."""
    non_null = [v for v in samples if v is not None]
    if not non_null:
        # No data to inspect: fall back to name heuristic.
        if DATE_COLUMN_HINTS.search(name or ""):
            return "date"
        return "unknown"

    if all(isinstance(v, DATE_LIKE_TYPES) for v in non_null):
        return "date"

    # Some Trino types come back as ISO strings; treat those as dates too.
    if all(isinstance(v, str) for v in non_null) and DATE_COLUMN_HINTS.search(name or ""):
        return "date"

    if all(isinstance(v, NUMERIC_TYPES) and not isinstance(v, bool) for v in non_null):
        # Floats that are NaN make a chart unhappy; treat as numeric anyway.
        return "numeric"

    # Strings, bools, or mixed -> treat as categorical bucket.
    return "categorical"


def _distinct_count(samples: List[Any]) -> int:
    return len({v for v in samples if v is not None})


def suggest_chart(
    columns: List[str],
    rows: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Return a chart suggestion or None when only a table is appropriate."""
    if not columns or not rows:
        return None

    # Sample at most 200 rows to keep this O(1) on big results.
    sample = rows[:200]

    per_col: Dict[str, str] = {}
    samples_by_col: Dict[str, List[Any]] = {col: [] for col in columns}
    for row in sample:
        for col in columns:
            samples_by_col[col].append(row.get(col))
    for col in columns:
        per_col[col] = _classify_column(col, samples_by_col[col])

    date_cols = [c for c in columns if per_col[c] == "date"]
    numeric_cols = [c for c in columns if per_col[c] == "numeric"]
    categorical_cols = [c for c in columns if per_col[c] == "categorical"]

    # Rule 1 / 2: date + numeric -> line
    if len(date_cols) == 1 and len(numeric_cols) >= 1:
        x = date_cols[0]
        if len(numeric_cols) == 1:
            return {"chart_type": "line", "x": x, "y": numeric_cols[0]}
        return {
            "chart_type": "line",
            "x": x,
            "y": numeric_cols[0],
            "series": numeric_cols[1:],
        }

    if len(categorical_cols) == 1 and len(numeric_cols) >= 1:
        cat = categorical_cols[0]
        cat_distinct = _distinct_count(samples_by_col[cat])

        # Rule 4: small categorical + part-of metric -> pie
        if cat_distinct <= 8 and any(PCT_COLUMN_HINTS.search(c) for c in numeric_cols):
            y = next(c for c in numeric_cols if PCT_COLUMN_HINTS.search(c))
            return {"chart_type": "pie", "x": cat, "y": y}

        # Rule 3: bar
        if cat_distinct <= 20:
            return {"chart_type": "bar", "x": cat, "y": numeric_cols[0], "sort": "desc"}

    # Rule 5: two numerics, no date -> scatter
    if not date_cols and len(numeric_cols) >= 2 and not categorical_cols:
        return {"chart_type": "scatter", "x": numeric_cols[0], "y": numeric_cols[1]}

    # Otherwise: table only.
    return None


__all__ = ["suggest_chart"]
