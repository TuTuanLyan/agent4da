"""Text-to-SQL (ported from code/api/llm_sql.py).

Catalog standardized to `iceberg`. Tries deterministic SQL first (fast, safe,
no LLM), then falls back to the OpenAI-compatible Groq client. Also resolves
which time filter was actually applied for the response trace.
"""

from __future__ import annotations

import re
from typing import Any

from . import llm
from .config import GOLD_CATALOG, GOLD_PREFIX, GOLD_SCHEMA, GOLD_TABLES

SEMANTIC_TABLE_CATALOG = f"{GOLD_CATALOG}.metadata.semantic_table_catalog"
SEMANTIC_COLUMN_CATALOG = f"{GOLD_CATALOG}.metadata.semantic_column_catalog"

SYSTEM_PROMPT = f"""You are a Trino SQL generator. Return SQL only. No markdown. No explanation.
Only generate SELECT, WITH, SHOW TABLES, or DESCRIBE queries.

Use only these Trino tables:

1. {GOLD_PREFIX}.daily_event_summary(
event_date, total_events, total_views, total_carts, total_remove_from_carts,
total_purchases, unique_users, unique_sessions, unique_products,
total_revenue, avg_event_price, conversion_rate, cart_to_purchase_rate,
gold_processed_at
)

2. {GOLD_PREFIX}.daily_brand_summary(
summary_id, event_date, brand, view_count, cart_count, purchase_count,
remove_from_cart_count, unique_events, unique_users, unique_products,
revenue, conversion_rate, cart_to_purchase_rate, gold_processed_at
)

3. {GOLD_PREFIX}.daily_category_summary(
summary_id, event_date, category_l1, category_l2, category_l3,
total_events, view_count, cart_count, purchase_count, remove_from_cart_count,
unique_events, unique_users, unique_products, revenue, conversion_rate,
cart_to_purchase_rate, gold_processed_at
)

4. {GOLD_PREFIX}.daily_product_summary(
summary_id, event_date, product_id, brand, category_l1, category_l2, category_l3,
view_count, cart_count, purchase_count, remove_from_cart_count,
unique_events, unique_users, unique_sessions, revenue, avg_price, min_price,
max_price, conversion_rate, cart_to_purchase_rate, gold_processed_at
)

5. {GOLD_PREFIX}.fact_events(
event_id, event_fingerprint, source_event_id, time_id, event_ts, event_date,
event_type, product_id, user_id, session_id, price, is_view, is_cart,
is_remove_from_cart, is_purchase, gold_processed_at
)

6. {GOLD_PREFIX}.fact_sales(
sale_id, event_fingerprint, source_event_id, time_id, sale_ts, sale_date,
product_id, user_id, session_id, unit_price, quantity, gross_amount,
gold_processed_at
)

7. {GOLD_PREFIX}.dim_time(time_id, event_date, event_year, event_month, event_day, event_hour, day_of_week, day_name, month_name, quarter, is_weekend)
8. {GOLD_PREFIX}.dim_product(product_id, category_id, category_code, category_l1, category_l2, category_l3, brand, avg_observed_price, min_observed_price, max_observed_price)
9. {GOLD_PREFIX}.dim_user(user_id, total_sessions, total_events, total_views, total_cart_adds, total_remove_from_carts, total_purchases, total_revenue)
10. {GOLD_PREFIX}.dim_session(session_id, user_id, session_start_at, session_end_at, session_duration_sec, event_count, view_count, cart_count, purchase_count, session_revenue, has_purchase)

Business rules:
- The real Gold catalog/schema is {GOLD_PREFIX}. Do not use any other catalog or schema.
- Metadata questions are allowed and should use:
  SELECT table_name FROM {SEMANTIC_TABLE_CATALOG} WHERE is_agent_visible = true ORDER BY table_name
  or SELECT column_name, data_type FROM {SEMANTIC_COLUMN_CATALOG} WHERE is_agent_visible = true AND table_name = '<table>' ORDER BY column_name.
- Do not use SHOW TABLES or {GOLD_CATALOG}.information_schema for metadata questions.
- Prefer Gold summary tables. Do not query Bronze or Silver unless the user explicitly asks to debug data quality or the pipeline.
- Daily aggregate questions use daily_event_summary.
- Brand questions use daily_brand_summary.
- Category questions use daily_category_summary.
- Product/top product questions use daily_product_summary.
- Drill-down/detail questions may use fact_events or fact_sales, but must include LIMIT.
- For revenue questions, use total_revenue in daily_event_summary, revenue in product/category/brand summaries, or gross_amount in fact_sales only for drill-down.
- For purchase questions, use total_purchases in daily_event_summary, or purchase_count in product/category/brand summaries.
- For view questions, use total_views in daily_event_summary or view_count in summaries.
- For cart questions, use total_carts in daily_event_summary or cart_count in summaries.
- For conversion questions, use conversion_rate.
- For daily trend questions, order by event_date.
- For highest/top/best brand revenue questions, select brand and SUM(revenue), GROUP BY brand, ORDER BY SUM(revenue) DESC.
- For highest/top/best category questions, group by category_l1, category_l2, category_l3 as needed.
- For top product revenue questions, group by product_id, brand and order by SUM(revenue) DESC.
- For brand comparison by day, filter the requested brands and ORDER BY event_date, brand.
- Use Trino SQL.
- Never generate INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE, CREATE, MERGE, CALL, GRANT, or REVOKE.
- Do not use SELECT * on fact/detail tables unless a LIMIT is present.
- Add LIMIT 20 for detail queries."""


def _format_schema_context(metadata_context: dict | None) -> str:
    if not metadata_context:
        return ""

    lines = []
    for table in metadata_context.get("tables", []):
        columns = metadata_context.get("columns", {}).get(table, [])
        column_text = ", ".join(f"{column['name']} {column['type']}" for column in columns)
        lines.append(f"- {GOLD_PREFIX}.{table}({column_text})")
    return "\n".join(lines)


def _build_focused_prompt(intent_result: dict | None, metadata_context: dict | None) -> str:
    schema_context = _format_schema_context(metadata_context)
    if not schema_context:
        return SYSTEM_PROMPT

    intent = (intent_result or {}).get("intent")
    dimension = (intent_result or {}).get("dimension")
    metric = (intent_result or {}).get("metric")
    limit = (intent_result or {}).get("limit", 20)
    analysis_type = (intent_result or {}).get("analysis_type")
    time_grain = (intent_result or {}).get("time_grain")
    time_range = (intent_result or {}).get("time_range")
    sort_direction = (intent_result or {}).get("sort_direction")
    comparison_entities = (intent_result or {}).get("comparison_entities") or []
    filters = (intent_result or {}).get("filters") or []

    return f"""You are a Trino SQL generator. Return SQL only. No markdown. No explanation.
Only generate SELECT or WITH queries.

Use only these selected Gold tables:
{schema_context}

Intent context:
- intent: {intent}
- analysis_type: {analysis_type}
- dimension: {dimension}
- metric: {metric}
- time_grain: {time_grain}
- time_range: {time_range}
- comparison_entities: {comparison_entities}
- filters: {filters}
- sort_direction: {sort_direction}
- preferred limit: {limit}

Rules:
- The real Gold catalog/schema is {GOLD_PREFIX}. Do not use any other catalog or schema.
- Never use postgresql.gold, analytics_test, Bronze, or Silver.
- Prefer the selected table candidates above; do not introduce other tables unless absolutely necessary.
- Brand questions should use daily_brand_summary when available.
- Category questions should use daily_category_summary when available.
- Product questions should use daily_product_summary when available.
- Overview and daily KPI questions should use daily_event_summary when available.
- Revenue overview should use total_revenue in daily_event_summary.
- Event overview should use total_events in daily_event_summary when available.
- Brand/category/product revenue should use revenue in the matching summary table.
- Brand event ranking should use unique_events in daily_brand_summary when total_events is absent.
- Comparison questions should filter the extracted entities in the matching dimension column.
- Breakdown/funnel questions should return the relevant metric columns from daily_event_summary when possible.
- Detail event questions should use fact_events and include LIMIT.
- Sales detail questions should use fact_sales and include LIMIT.
- Use Trino SQL.
- Never generate INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE, CREATE, MERGE, CALL, GRANT, or REVOKE.
- Do not use SELECT * on fact/detail tables unless a LIMIT is present.
- Add LIMIT {limit} for non-aggregate detail queries."""


def _metadata_sql_for_question(question: str) -> str | None:
    normalized = question.strip().lower()

    asks_for_tables = (
        "bảng gold" in normalized
        or "bang gold" in normalized
        or ("gold" in normalized and "bảng" in normalized)
        or ("gold" in normalized and "bang" in normalized)
        or ("gold" in normalized and "tables" in normalized)
    )
    if asks_for_tables:
        table_expr = (
            f"CASE WHEN starts_with(table_name, '{GOLD_SCHEMA}.') "
            f"THEN substr(table_name, {len(GOLD_SCHEMA) + 2}) "
            "ELSE table_name END"
        )
        return (
            f"SELECT {table_expr} AS table_name "
            f"FROM {SEMANTIC_TABLE_CATALOG} "
            "WHERE is_agent_visible = true "
            "ORDER BY table_name"
        )

    asks_for_columns = (
        "cột" in normalized
        or "cot" in normalized
        or "columns" in normalized
        or "schema" in normalized
    )
    if asks_for_columns:
        for table in GOLD_TABLES:
            if table in normalized:
                qualified_table_name = f"{GOLD_SCHEMA}.{table}"
                return (
                    "SELECT DISTINCT column_name, data_type "
                    f"FROM {SEMANTIC_COLUMN_CATALOG} "
                    "WHERE is_agent_visible = true "
                    f"AND table_name IN ('{table}', '{qualified_table_name}') "
                    "ORDER BY column_name"
                )

    return None


def _clean_sql_response(sql: str) -> str:
    sql = sql.strip()
    fence_match = re.fullmatch(r"```(?:sql)?\s*(.*?)\s*```", sql, flags=re.IGNORECASE | re.DOTALL)
    if fence_match:
        sql = fence_match.group(1)

    sql = sql.strip()
    if sql.endswith(";"):
        sql = sql[:-1].rstrip()
    return sql


def _columns_for_table(metadata_context: dict | None, table_name: str) -> set[str]:
    columns = (metadata_context or {}).get("columns", {}).get(table_name, [])
    return {str(column.get("name")) for column in columns if column.get("name")}


def _first_available(candidates: tuple[str, ...], available_columns: set[str]) -> str:
    for column_name in candidates:
        if column_name in available_columns:
            return column_name
    return candidates[0]


def _metric_column(metric: str | None, table_name: str, metadata_context: dict | None) -> str:
    available_columns = _columns_for_table(metadata_context, table_name)
    normalized = metric or "total_events"
    candidates_by_metric = {
        "revenue": ("total_revenue", "revenue", "gross_amount"),
        "total_revenue": ("total_revenue", "revenue", "gross_amount"),
        "total_events": ("total_events", "unique_events", "event_count", "count", "cnt"),
        "event": ("total_events", "unique_events", "event_count", "count", "cnt"),
        "unique_events": ("unique_events", "total_events", "event_count"),
        "total_views": ("total_views", "view_count"),
        "view": ("total_views", "view_count"),
        "total_carts": ("total_carts", "cart_count"),
        "cart": ("total_carts", "cart_count"),
        "total_purchases": ("total_purchases", "purchase_count"),
        "purchase": ("total_purchases", "purchase_count"),
        "conversion": ("conversion_rate", "cart_to_purchase_rate"),
        "count": ("count", "cnt", "event_count", "total_events", "unique_events"),
    }
    candidates = candidates_by_metric.get(normalized, (normalized,))
    return _first_available(candidates, available_columns) if available_columns else candidates[0]


def _dimension_column(dimension: str | None, table_name: str, metadata_context: dict | None) -> str:
    available_columns = _columns_for_table(metadata_context, table_name)
    normalized = dimension or ""
    candidates_by_dimension = {
        "brand": ("brand",),
        "category": ("category_l1", "category_code", "category_l2", "category_l3"),
        "product": ("product_id", "product"),
        "event_type": ("event_type",),
        "event_date": ("event_date", "sale_date", "date", "day"),
        "date": ("event_date", "sale_date", "date", "day"),
    }
    candidates = candidates_by_dimension.get(normalized, (normalized,))
    return _first_available(candidates, available_columns) if available_columns else candidates[0]


def _sql_literal(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _selected_table(intent_result: dict | None, fallback: str = "daily_event_summary") -> str:
    candidates = (intent_result or {}).get("table_candidates") or []
    return candidates[0] if candidates else fallback


def _metric_list(intent_result: dict | None, table_name: str, metadata_context: dict | None) -> list[str]:
    extracted = (intent_result or {}).get("extracted_entities") or {}
    metrics = extracted.get("metrics") or []
    if not metrics:
        metrics = [(intent_result or {}).get("metric") or "total_events"]
    columns = []
    for metric in metrics:
        column_name = _metric_column(metric, table_name, metadata_context)
        if column_name not in columns:
            columns.append(column_name)
    return columns


TIME_FILTER_COLUMNS = ("event_date", "sale_date", "date")


def _normalize_time_range(time_range: Any) -> dict[str, Any] | None:
    if isinstance(time_range, dict) and time_range.get("type"):
        return dict(time_range)
    if isinstance(time_range, str) and time_range:
        return {"type": time_range, "field": "event_date"}
    return None


def _time_column(table_name: str, metadata_context: dict | None, preferred_field: str | None = None) -> str | None:
    available_columns = _columns_for_table(metadata_context, table_name)
    if not available_columns:
        return None

    if preferred_field and preferred_field in available_columns and preferred_field in TIME_FILTER_COLUMNS:
        return preferred_field

    for column_name in TIME_FILTER_COLUMNS:
        if column_name in available_columns:
            return column_name
    return None


def _valid_date_literal(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    return value if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value) else None


def _time_filter_condition(
    table_name: str,
    metadata_context: dict | None,
    time_range: Any,
) -> tuple[str | None, dict[str, Any] | None]:
    normalized = _normalize_time_range(time_range)
    if not normalized:
        return None, None

    column_name = _time_column(table_name, metadata_context, normalized.get("field"))
    if not column_name:
        return None, None

    range_type = normalized.get("type")
    applied: dict[str, Any] = {"type": range_type, "field": column_name}

    if range_type == "latest":
        return (
            f"{column_name} = (SELECT MAX({column_name}) FROM {GOLD_PREFIX}.{table_name})",
            applied,
        )

    if range_type == "exact_date":
        start = _valid_date_literal(normalized.get("start"))
        if not start:
            return None, None
        applied.update({"start": start, "end": normalized.get("end") or start})
        return f"{column_name} = DATE '{start}'", applied

    if range_type == "date_range":
        start = _valid_date_literal(normalized.get("start"))
        end = _valid_date_literal(normalized.get("end"))
        if not start or not end:
            return None, None
        applied.update({"start": start, "end": end})
        return f"{column_name} BETWEEN DATE '{start}' AND DATE '{end}'", applied

    if range_type == "today":
        return f"{column_name} = CURRENT_DATE", applied

    if range_type == "yesterday":
        return f"{column_name} = CURRENT_DATE - INTERVAL '1' DAY", applied

    if range_type == "this_week":
        return (
            f"{column_name} BETWEEN CAST(date_trunc('week', CURRENT_DATE) AS date) AND CURRENT_DATE",
            applied,
        )

    return None, None


def _where_clause(condition: str | None) -> str:
    return f" WHERE {condition}" if condition else ""


def _and_conditions(conditions: list[str | None]) -> str:
    active_conditions = [condition for condition in conditions if condition]
    return " AND ".join(active_conditions)


# Rate/price metrics must be averaged, not summed, when aggregating a dimension.
_AVERAGE_METRIC_COLUMNS = {
    "conversion_rate",
    "cart_to_purchase_rate",
    "avg_event_price",
    "avg_price",
}


def _aggregate_expr(metric_column: str) -> str:
    if metric_column in _AVERAGE_METRIC_COLUMNS or metric_column.endswith("_rate"):
        return f"AVG({metric_column})"
    return f"SUM({metric_column})"


def _filter_conditions(
    filters: list[dict] | None,
    table_name: str,
    metadata_context: dict | None,
) -> str | None:
    """Build case-insensitive IN / NOT IN conditions from NLU filters.

    Used for exclusion follow-ups ("bỏ qua nhãn hàng unknown" -> NOT IN) and
    explicit inclusions. Unknown fields/empty values are skipped so a bad filter
    never produces invalid SQL.
    """
    # Only emit a filter when we can confirm the column exists on the table, so a
    # brand exclusion never leaks onto daily_event_summary (which has no brand).
    available_columns = _columns_for_table(metadata_context, table_name)
    if not available_columns:
        return None
    conditions: list[str] = []
    for spec in filters or []:
        field = spec.get("field")
        operator = (spec.get("operator") or "").lower()
        values = [value for value in (spec.get("values") or []) if str(value).strip() != ""]
        # Numeric metric thresholds (field "__metric__") are HAVING clauses,
        # built separately by _having_conditions; skip them here.
        if field == "__metric__" or operator in (">", ">=", "<", "<=", "between"):
            continue
        if not field or not values:
            continue
        column = _dimension_column(field, table_name, metadata_context)
        if not column or column not in available_columns:
            continue
        value_sql = ", ".join(_sql_literal(str(value).lower()) for value in values)
        if operator in ("not_in", "!=", "<>", "ne", "exclude", "not"):
            conditions.append(f"lower({column}) NOT IN ({value_sql})")
        elif operator in ("in", "=", "eq", "include"):
            conditions.append(f"lower({column}) IN ({value_sql})")
    return _and_conditions(conditions) or None


def _having_conditions(filters: list[dict] | None, aggregate_expr: str) -> str | None:
    """Build a HAVING clause from numeric metric thresholds (field "__metric__").

    e.g. "trên 1000 view" on a ranking -> HAVING SUM(view_count) > 1000.
    """
    allowed = {">", ">=", "<", "<="}
    conditions: list[str] = []
    for spec in filters or []:
        if spec.get("field") != "__metric__":
            continue
        operator = (spec.get("operator") or "").lower()
        if operator not in allowed:
            continue
        values = spec.get("values") or []
        if not values:
            continue
        try:
            number = float(values[0])
        except (TypeError, ValueError):
            continue
        literal = str(int(number)) if number.is_integer() else repr(number)
        conditions.append(f"{aggregate_expr} {operator} {literal}")
    return _and_conditions(conditions) or None


def resolve_applied_time_filter(
    intent_result: dict | None,
    metadata_context: dict | None,
    generated_sql: str,
) -> dict[str, Any] | None:
    if not intent_result or not generated_sql:
        return None

    table_name = _selected_table(intent_result, "daily_event_summary")
    if intent_result.get("intent") == "trend" or (
        intent_result.get("intent") == "revenue_sales"
        and intent_result.get("analysis_type") == "time_series"
    ):
        table_name = "daily_event_summary"
    if intent_result.get("intent") == "conversion_funnel":
        table_name = "daily_event_summary"

    _condition, applied = _time_filter_condition(
        table_name,
        metadata_context,
        intent_result.get("time_range"),
    )
    if not applied:
        return None

    sql = generated_sql.lower()
    column_name = applied["field"].lower()
    range_type = applied["type"]
    if column_name not in sql:
        return None
    if range_type == "latest" and "max(" in sql:
        return applied
    if range_type == "exact_date" and f"date '{applied.get('start')}'" in sql:
        return applied
    if range_type == "date_range" and " between " in sql and f"date '{applied.get('start')}'" in sql and f"date '{applied.get('end')}'" in sql:
        return applied
    if range_type in {"today", "yesterday", "this_week"} and "current_date" in sql:
        return applied
    return None


def _deterministic_sql(
    question: str,
    intent_result: dict | None,
    metadata_context: dict | None,
) -> str | None:
    if not intent_result:
        return None

    intent = intent_result.get("intent")
    analysis_type = intent_result.get("analysis_type")
    metric = intent_result.get("metric")
    dimension = intent_result.get("dimension")
    limit = max(1, min(int(intent_result.get("limit") or 10), 100))

    if intent == "comparison":
        table_name = _selected_table(intent_result, "daily_brand_summary")
        dimension_column = _dimension_column(dimension, table_name, metadata_context)
        metric_column = _metric_column(metric, table_name, metadata_context)
        entities = intent_result.get("comparison_entities") or []
        if not entities:
            return None
        entity_sql = ", ".join(_sql_literal(entity.lower()) for entity in entities)
        time_condition, _applied = _time_filter_condition(table_name, metadata_context, intent_result.get("time_range"))
        # The entity IN-filter is already rendered above; only carry exclusions here.
        exclusion_specs = [
            spec
            for spec in (intent_result.get("filters") or [])
            if (spec.get("operator") or "").lower() in ("not_in", "!=", "<>", "ne", "exclude", "not")
        ]
        exclusion_condition = _filter_conditions(exclusion_specs, table_name, metadata_context)
        where_condition = _and_conditions(
            [f"lower({dimension_column}) IN ({entity_sql})", time_condition, exclusion_condition]
        )
        return (
            f"SELECT {dimension_column}, SUM({metric_column}) AS {metric_column} "
            f"FROM {GOLD_PREFIX}.{table_name} "
            f"WHERE {where_condition} "
            f"GROUP BY {dimension_column} "
            f"ORDER BY {metric_column} DESC "
            f"LIMIT {limit}"
        )

    if intent == "breakdown":
        table_name = _selected_table(intent_result, "daily_event_summary")
        metrics = _metric_list(intent_result, table_name, metadata_context)
        time_condition, _applied = _time_filter_condition(table_name, metadata_context, intent_result.get("time_range"))
        filter_condition = _filter_conditions(intent_result.get("filters"), table_name, metadata_context)
        where_clause = _where_clause(_and_conditions([time_condition, filter_condition]))
        if table_name == "daily_event_summary" and len(metrics) > 1:
            return f"SELECT {', '.join(metrics)} FROM {GOLD_PREFIX}.{table_name}{where_clause} LIMIT {limit}"
        if dimension == "event_type":
            event_time_condition, _event_applied = _time_filter_condition("fact_events", metadata_context, intent_result.get("time_range"))
            event_filter_condition = _filter_conditions(intent_result.get("filters"), "fact_events", metadata_context)
            return (
                "SELECT event_type, COUNT(*) AS event_count "
                f"FROM {GOLD_PREFIX}.fact_events "
                f"{_where_clause(_and_conditions([event_time_condition, event_filter_condition]))} "
                "GROUP BY event_type "
                "ORDER BY event_count DESC "
                f"LIMIT {limit}"
            )
        return None

    if intent == "conversion_funnel":
        time_condition, _applied = _time_filter_condition("daily_event_summary", metadata_context, intent_result.get("time_range"))
        return (
            "SELECT total_views, total_carts, total_purchases "
            f"FROM {GOLD_PREFIX}.daily_event_summary "
            f"{_where_clause(time_condition)} "
            f"LIMIT {limit}"
        )

    if intent == "trend" or (intent == "revenue_sales" and analysis_type == "time_series"):
        table_name = "daily_event_summary"
        time_column = _dimension_column("event_date", table_name, metadata_context)
        metric_column = _metric_column(metric, table_name, metadata_context)
        time_condition, _applied = _time_filter_condition(table_name, metadata_context, intent_result.get("time_range"))
        return (
            f"SELECT {time_column}, {metric_column} "
            f"FROM {GOLD_PREFIX}.{table_name} "
            f"{_where_clause(time_condition)} "
            f"ORDER BY {time_column} "
            f"LIMIT {limit}"
        )

    if intent == "ranking":
        table_name = _selected_table(intent_result, "daily_event_summary")
        dimension_column = _dimension_column(dimension, table_name, metadata_context)
        direction = "ASC" if intent_result.get("sort_direction") == "asc" else "DESC"
        if not dimension_column:
            return None
        # Summary tables hold one row per (date, dimension); aggregate so a
        # ranking reflects the whole period (and so any year/exclusion filter is
        # applied before grouping), e.g. top brands by total views in 2020.
        metric_columns = _metric_list(intent_result, table_name, metadata_context)
        primary_metric = metric_columns[0] if metric_columns else _metric_column(metric, table_name, metadata_context)
        select_aggregates = ", ".join(f"{_aggregate_expr(col)} AS {col}" for col in metric_columns or [primary_metric])
        primary_aggregate = _aggregate_expr(primary_metric)
        time_condition, _applied = _time_filter_condition(table_name, metadata_context, intent_result.get("time_range"))
        filter_condition = _filter_conditions(intent_result.get("filters"), table_name, metadata_context)
        where_condition = _and_conditions([time_condition, filter_condition])
        having_condition = _having_conditions(intent_result.get("filters"), primary_aggregate)
        having_clause = f" HAVING {having_condition}" if having_condition else ""
        return (
            f"SELECT {dimension_column}, {select_aggregates} "
            f"FROM {GOLD_PREFIX}.{table_name}"
            f"{_where_clause(where_condition)} "
            f"GROUP BY {dimension_column}"
            f"{having_clause} "
            f"ORDER BY {primary_metric} {direction} "
            f"LIMIT {limit}"
        )

    if intent == "metric_overview":
        table_name = "daily_event_summary"
        metrics = _metric_list(intent_result, table_name, metadata_context)
        if len(metrics) == 1 and metrics[0] in {"total_views", "total_carts", "total_purchases"}:
            metrics = ["total_views", "total_carts"]
        time_condition, _applied = _time_filter_condition(table_name, metadata_context, intent_result.get("time_range"))
        return f"SELECT {', '.join(metrics)} FROM {GOLD_PREFIX}.{table_name}{_where_clause(time_condition)} LIMIT {limit}"

    return None


def _followup_user_prompt(question: str, previous_sql: str | None) -> str:
    if not previous_sql:
        return f"Question: {question}"
    normalized_previous = " ".join(previous_sql.split())
    return (
        f"Question: {question}\n\n"
        "This is a follow-up to a previous question in the same conversation. "
        "The previous related query was:\n"
        f"{normalized_previous}\n\n"
        "Adjust that query to satisfy the new question - keep the parts that still "
        "apply (metric, dimension, time filter) and add or change only what the new "
        "message asks for (e.g. an exclusion/filter, a new time range, an extra "
        "column, a different sort). Return SQL only."
    )


def generate_sql(
    question: str,
    intent_result: dict | None = None,
    metadata_context: dict | None = None,
    *,
    prefer_llm: bool = False,
    previous_sql: str | None = None,
) -> str:
    """Generate read-only Trino SQL for a question.

    `prefer_llm` is set for conversational follow-ups: the deterministic
    templates cannot express open-ended refinements (exclusions, ad-hoc filters,
    extra columns), so the LLM generates the SQL instead, optionally refining the
    `previous_sql` from the prior turn. Falls back to the deterministic path when
    no Groq key is configured so behaviour degrades instead of failing.
    """
    metadata_sql = _metadata_sql_for_question(question)
    if metadata_sql:
        return metadata_sql

    if not prefer_llm:
        deterministic_sql = _deterministic_sql(question, intent_result, metadata_context)
        if deterministic_sql:
            return deterministic_sql

    if not llm.llm_available():
        # When the LLM was preferred (a follow-up) but no key is set, fall back
        # to the deterministic templates rather than failing the whole turn.
        if prefer_llm:
            deterministic_sql = _deterministic_sql(question, intent_result, metadata_context)
            if deterministic_sql:
                return deterministic_sql
        raise ValueError("GROQ_API_KEY is not set")

    content = llm.chat_completion(
        [
            {"role": "system", "content": _build_focused_prompt(intent_result, metadata_context)},
            {"role": "user", "content": _followup_user_prompt(question, previous_sql if prefer_llm else None)},
        ],
        temperature=0,
        max_tokens=300,
    )
    sql = _clean_sql_response(content)
    if not sql:
        raise ValueError("LLM returned an empty SQL response")
    return sql
