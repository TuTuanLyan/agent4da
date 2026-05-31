import os
import re
from typing import Any

from dotenv import load_dotenv
from groq import Groq

load_dotenv()

DEFAULT_MODEL = "llama-3.3-70b-versatile"
GOLD_CATALOG = "iceberg_catalog"
GOLD_SCHEMA = "gold"
SEMANTIC_TABLE_CATALOG = f"{GOLD_CATALOG}.metadata.semantic_table_catalog"
SEMANTIC_COLUMN_CATALOG = f"{GOLD_CATALOG}.metadata.semantic_column_catalog"
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

SYSTEM_PROMPT = """You are a Trino SQL generator. Return SQL only. No markdown. No explanation.
Only generate SELECT, WITH, SHOW TABLES, or DESCRIBE queries.

Use only these Trino tables:

1. iceberg_catalog.gold.daily_event_summary(
event_date, total_events, total_views, total_carts, total_remove_from_carts,
total_purchases, unique_users, unique_sessions, unique_products,
total_revenue, avg_event_price, conversion_rate, cart_to_purchase_rate,
gold_processed_at
)

2. iceberg_catalog.gold.daily_brand_summary(
summary_id, event_date, brand, view_count, cart_count, purchase_count,
remove_from_cart_count, unique_events, unique_users, unique_products,
revenue, conversion_rate, cart_to_purchase_rate, gold_processed_at
)

3. iceberg_catalog.gold.daily_category_summary(
summary_id, event_date, category_l1, category_l2, category_l3,
total_events, view_count, cart_count, purchase_count, remove_from_cart_count,
unique_events, unique_users, unique_products, revenue, conversion_rate,
cart_to_purchase_rate, gold_processed_at
)

4. iceberg_catalog.gold.daily_product_summary(
summary_id, event_date, product_id, brand, category_l1, category_l2, category_l3,
view_count, cart_count, purchase_count, remove_from_cart_count,
unique_events, unique_users, unique_sessions, revenue, avg_price, min_price,
max_price, conversion_rate, cart_to_purchase_rate, gold_processed_at
)

5. iceberg_catalog.gold.fact_events(
event_id, event_fingerprint, source_event_id, time_id, event_ts, event_date,
event_type, product_id, user_id, session_id, price, is_view, is_cart,
is_remove_from_cart, is_purchase, gold_processed_at
)

6. iceberg_catalog.gold.fact_sales(
sale_id, event_fingerprint, source_event_id, time_id, sale_ts, sale_date,
product_id, user_id, session_id, unit_price, quantity, gross_amount,
gold_processed_at
)

7. iceberg_catalog.gold.dim_time(time_id, event_date, event_year, event_month, event_day, event_hour, day_of_week, day_name, month_name, quarter, is_weekend)
8. iceberg_catalog.gold.dim_product(product_id, category_id, category_code, category_l1, category_l2, category_l3, brand, avg_observed_price, min_observed_price, max_observed_price)
9. iceberg_catalog.gold.dim_user(user_id, total_sessions, total_events, total_views, total_cart_adds, total_remove_from_carts, total_purchases, total_revenue)
10. iceberg_catalog.gold.dim_session(session_id, user_id, session_start_at, session_end_at, session_duration_sec, event_count, view_count, cart_count, purchase_count, session_revenue, has_purchase)

Business rules:
- The real Gold catalog/schema is iceberg_catalog.gold. Do not use any other catalog or schema.
- Metadata questions are allowed and should use:
  SELECT table_name FROM iceberg_catalog.metadata.semantic_table_catalog WHERE is_agent_visible = true ORDER BY table_name
  or SELECT column_name, data_type FROM iceberg_catalog.metadata.semantic_column_catalog WHERE is_agent_visible = true AND table_name = '<table>' ORDER BY column_name.
- Do not use SHOW TABLES or iceberg_catalog.information_schema for metadata questions.
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

    lines = ["TECHNICAL GOLD SCHEMA"]
    for table in metadata_context.get("tables", []):
        columns = metadata_context.get("columns", {}).get(table, [])
        column_text = ", ".join(
            f"{column['name']} {column['type']}"
            for column in columns
        )
        lines.append(f"- iceberg_catalog.gold.{table}({column_text})")

    if metadata_context.get("semantic_available"):
        lines.extend(["", "SEMANTIC BUSINESS METADATA"])
        semantic_columns = metadata_context.get("semantic_columns", {})
        for table in metadata_context.get("semantic_tables", []):
            table_key = table.get("table_key") or str(table.get("table_name", "")).split(".")[-1]
            lines.extend(
                [
                    f"Table: iceberg_catalog.gold.{table_key}",
                    f"Display name: {table.get('display_name') or ''}",
                    f"Purpose: {table.get('purpose') or ''}",
                    f"Grain: {table.get('grain') or ''}",
                    f"Use for: {table.get('use_for') or ''}",
                    f"Query notes: {table.get('query_notes') or ''}",
                    "Business columns:",
                ]
            )
            for column in semantic_columns.get(table_key, []):
                lines.append(
                    "- {name} ({data_type}): {meaning} Terms: {terms} Usage: {usage}".format(
                        name=column.get("column_name") or column.get("name"),
                        data_type=column.get("data_type") or column.get("type") or "",
                        meaning=column.get("meaning") or "",
                        terms=column.get("business_terms") or "",
                        usage=column.get("example_usage") or "",
                    )
                )
            lines.append("")
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
- The real Gold catalog/schema is iceberg_catalog.gold. Do not use any other catalog or schema.
- Never use postgresql.gold, analytics_test, Bronze, or Silver.
- Prefer the selected table candidates above; do not introduce other tables unless absolutely necessary.
- If semantic business metadata is provided, use its purpose, grain, use_for, query_notes, column meaning, business_terms, and example_usage to choose tables and columns.
- Respect table grain. If a daily summary table has grain by event_date plus a dimension and the question asks for an overall ranking by that dimension, aggregate the metric with SUM and GROUP BY the dimension.
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
            f"{column_name} = (SELECT MAX({column_name}) FROM iceberg_catalog.gold.{table_name})",
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
        time_condition, _applied_time_filter = _time_filter_condition(
            table_name,
            metadata_context,
            intent_result.get("time_range"),
        )
        where_condition = _and_conditions([f"lower({dimension_column}) IN ({entity_sql})", time_condition])
        return (
            f"SELECT {dimension_column}, SUM({metric_column}) AS {metric_column} "
            f"FROM iceberg_catalog.gold.{table_name} "
            f"WHERE {where_condition} "
            f"GROUP BY {dimension_column} "
            f"ORDER BY {metric_column} DESC "
            f"LIMIT {limit}"
        )

    if intent == "breakdown":
        table_name = _selected_table(intent_result, "daily_event_summary")
        metrics = _metric_list(intent_result, table_name, metadata_context)
        time_condition, _applied_time_filter = _time_filter_condition(
            table_name,
            metadata_context,
            intent_result.get("time_range"),
        )
        where_clause = _where_clause(time_condition)
        if table_name == "daily_event_summary" and len(metrics) > 1:
            return f"SELECT {', '.join(metrics)} FROM iceberg_catalog.gold.{table_name}{where_clause} LIMIT {limit}"
        if dimension == "event_type":
            event_time_condition, _event_applied_time_filter = _time_filter_condition(
                "fact_events",
                metadata_context,
                intent_result.get("time_range"),
            )
            return (
                "SELECT event_type, COUNT(*) AS event_count "
                "FROM iceberg_catalog.gold.fact_events "
                f"{_where_clause(event_time_condition)} "
                "GROUP BY event_type "
                "ORDER BY event_count DESC "
                f"LIMIT {limit}"
            )
        return None

    if intent == "conversion_funnel":
        time_condition, _applied_time_filter = _time_filter_condition(
            "daily_event_summary",
            metadata_context,
            intent_result.get("time_range"),
        )
        return (
            "SELECT total_views, total_carts, total_purchases "
            "FROM iceberg_catalog.gold.daily_event_summary "
            f"{_where_clause(time_condition)} "
            f"LIMIT {limit}"
        )

    if intent == "trend" or (intent == "revenue_sales" and analysis_type == "time_series"):
        table_name = "daily_event_summary"
        time_column = _dimension_column("event_date", table_name, metadata_context)
        metric_column = _metric_column(metric, table_name, metadata_context)
        time_condition, _applied_time_filter = _time_filter_condition(
            table_name,
            metadata_context,
            intent_result.get("time_range"),
        )
        return (
            f"SELECT {time_column}, {metric_column} "
            f"FROM iceberg_catalog.gold.{table_name} "
            f"{_where_clause(time_condition)} "
            f"ORDER BY {time_column} "
            f"LIMIT {limit}"
        )

    if intent == "ranking":
        table_name = _selected_table(intent_result, "daily_event_summary")
        dimension_column = _dimension_column(dimension, table_name, metadata_context)
        metric_column = _metric_column(metric, table_name, metadata_context)
        direction = "ASC" if intent_result.get("sort_direction") == "asc" else "DESC"
        if not dimension_column:
            return None
        time_condition, _applied_time_filter = _time_filter_condition(
            table_name,
            metadata_context,
            intent_result.get("time_range"),
        )
        return (
            f"SELECT {dimension_column}, {metric_column} "
            f"FROM iceberg_catalog.gold.{table_name} "
            f"{_where_clause(time_condition)} "
            f"ORDER BY {metric_column} {direction} "
            f"LIMIT {limit}"
        )

    if intent == "metric_overview":
        table_name = "daily_event_summary"
        metrics = _metric_list(intent_result, table_name, metadata_context)
        if len(metrics) == 1 and metrics[0] in {"total_views", "total_carts", "total_purchases"}:
            metrics = ["total_views", "total_carts"]
        time_condition, _applied_time_filter = _time_filter_condition(
            table_name,
            metadata_context,
            intent_result.get("time_range"),
        )
        return f"SELECT {', '.join(metrics)} FROM iceberg_catalog.gold.{table_name}{_where_clause(time_condition)} LIMIT {limit}"

    return None


def generate_sql(
    question: str,
    intent_result: dict | None = None,
    metadata_context: dict | None = None,
) -> str:
    metadata_sql = _metadata_sql_for_question(question)
    if metadata_sql:
        return metadata_sql

    deterministic_sql = _deterministic_sql(question, intent_result, metadata_context)
    if deterministic_sql:
        return deterministic_sql

    api_key = os.getenv("GROQ_API_KEY")
    if not api_key or api_key == "your_groq_api_key_here":
        raise ValueError("GROQ_API_KEY is not set")

    client = Groq(api_key=api_key)
    completion = client.chat.completions.create(
        model=os.getenv("GROQ_MODEL", DEFAULT_MODEL),
        messages=[
            {"role": "system", "content": _build_focused_prompt(intent_result, metadata_context)},
            {"role": "user", "content": f"Question: {question}"},
        ],
        temperature=0,
        max_completion_tokens=250,
    )

    content = completion.choices[0].message.content or ""
    sql = _clean_sql_response(content)
    if not sql:
        raise ValueError("Groq returned an empty SQL response")
    return sql
