"""SQL self-correction (ported from code/api/sql_corrector.py).

Catalog standardized to `iceberg`. Heuristic fixes run first (disallowed
catalog, unresolved column, wrong table); the LLM fix is a fallback. DDL/DML and
unrecoverable infra errors are never retried.
"""

from __future__ import annotations

import re
from typing import Any

from . import llm
from .config import GOLD_PREFIX, MAX_SQL_RETRY_ATTEMPTS

DANGEROUS_KEYWORDS = (
    "INSERT",
    "UPDATE",
    "DELETE",
    "DROP",
    "ALTER",
    "TRUNCATE",
    "CREATE",
    "MERGE",
    "CALL",
    "GRANT",
    "REVOKE",
)
DISALLOWED_CATALOG_PATTERNS = (
    r"\bpostgresql\s*\.\s*gold\s*\.",
    r"\banalytics_test\s*\.",
    r"\bbronze\s*\.",
    r"\bsilver\s*\.",
)
COLUMN_FALLBACKS = {
    "total_revenue": ("revenue", "gross_amount"),
    "revenue": ("total_revenue", "gross_amount"),
    "gross_amount": ("revenue", "total_revenue"),
    "total_views": ("view_count",),
    "view_count": ("total_views",),
    "total_carts": ("cart_count",),
    "cart_count": ("total_carts",),
    "total_purchases": ("purchase_count",),
    "purchase_count": ("total_purchases",),
    "total_events": ("unique_events", "event_count"),
    "unique_events": ("total_events", "event_count"),
    "event_count": ("total_events", "unique_events"),
}


def _clean_sql_response(sql: str) -> str:
    sql = sql.strip()
    fence_match = re.fullmatch(r"```(?:sql)?\s*(.*?)\s*```", sql, flags=re.IGNORECASE | re.DOTALL)
    if fence_match:
        sql = fence_match.group(1)
    sql = sql.strip()
    while sql.endswith(";"):
        sql = sql[:-1].rstrip()
    return sql


def _contains_dangerous_keyword(sql: str) -> bool:
    return any(re.search(rf"\b{keyword}\b", sql, flags=re.IGNORECASE) for keyword in DANGEROUS_KEYWORDS)


def _all_columns(metadata_context: dict[str, Any]) -> set[str]:
    return {
        str(column["name"]).lower()
        for columns in metadata_context.get("columns", {}).values()
        for column in columns
        if column.get("name")
    }


def _format_allowed_schema(metadata_context: dict[str, Any], table_candidates: list[str]) -> str:
    lines = []
    selected_tables = metadata_context.get("tables") or table_candidates
    for table in selected_tables:
        columns = metadata_context.get("columns", {}).get(table, [])
        if columns:
            column_text = ", ".join(f"{column['name']} {column['type']}" for column in columns)
            lines.append(f"- {GOLD_PREFIX}.{table}({column_text})")
        else:
            lines.append(f"- {GOLD_PREFIX}.{table}")
    return "\n".join(lines) if lines else f"- {GOLD_PREFIX}.<allowed_gold_table>"


def _replace_table_reference(sql: str, table_candidates: list[str]) -> tuple[str, str | None]:
    if not table_candidates:
        return sql, None

    preferred_table = table_candidates[0]
    replacement = f"{GOLD_PREFIX}.{preferred_table}"
    pattern = re.compile(
        r"\b(FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){0,2})\b",
        flags=re.IGNORECASE,
    )
    changed = False

    def replace(match: re.Match[str]) -> str:
        nonlocal changed
        table_ref = match.group(2)
        normalized_ref = table_ref.lower()
        if normalized_ref == replacement:
            return match.group(0)
        if normalized_ref.startswith(f"{GOLD_PREFIX}.") and normalized_ref.rsplit(".", 1)[-1] == preferred_table:
            return match.group(0)
        changed = True
        return f"{match.group(1)} {replacement}"

    corrected = pattern.sub(replace, sql)
    if changed:
        return corrected, f"Replaced table reference with {GOLD_PREFIX}.{preferred_table}."
    return sql, None


def _replace_disallowed_catalog(sql: str, table_candidates: list[str]) -> tuple[str, str | None]:
    corrected = sql
    for table in table_candidates:
        corrected = re.sub(
            rf"\bpostgresql\s*\.\s*gold\s*\.\s*{table}\b",
            f"{GOLD_PREFIX}.{table}",
            corrected,
            flags=re.IGNORECASE,
        )
        corrected = re.sub(
            rf"\banalytics_test\s*\.\s*(?:gold\s*\.\s*)?{table}\b",
            f"{GOLD_PREFIX}.{table}",
            corrected,
            flags=re.IGNORECASE,
        )

    if corrected != sql:
        return corrected, f"Replaced disallowed catalog/schema with {GOLD_PREFIX}."
    return sql, None


def _missing_column_from_error(error_message: str) -> str | None:
    patterns = (
        r"Column\s+'([^']+)'\s+cannot\s+be\s+resolved",
        r'Column\s+"([^"]+)"\s+cannot\s+be\s+resolved',
        r"Column\s+([A-Za-z_][A-Za-z0-9_]*)\s+cannot\s+be\s+resolved",
    )
    for pattern in patterns:
        match = re.search(pattern, error_message, flags=re.IGNORECASE)
        if match:
            return match.group(1).lower()
    return None


def _best_column_replacement(missing_column: str, metadata_context: dict[str, Any]) -> str | None:
    columns = _all_columns(metadata_context)
    for candidate in COLUMN_FALLBACKS.get(missing_column, ()):
        if candidate in columns:
            return candidate

    if missing_column.startswith("total_"):
        without_prefix = missing_column.removeprefix("total_")
        if without_prefix in columns:
            return without_prefix

    for column in columns:
        if missing_column in column or column in missing_column:
            return column
    return None


def _replace_missing_column(sql: str, error_message: str, metadata_context: dict[str, Any]) -> tuple[str, str | None]:
    missing_column = _missing_column_from_error(error_message)
    if not missing_column:
        return sql, None

    replacement = _best_column_replacement(missing_column, metadata_context)
    if not replacement:
        return sql, None

    corrected = re.sub(rf"\b{re.escape(missing_column)}\b", replacement, sql, flags=re.IGNORECASE)
    if corrected != sql:
        return corrected, f"Replaced unresolved column {missing_column} with {replacement} from Gold metadata."
    return sql, None


def _heuristic_correction(
    *,
    failed_sql: str,
    error_message: str,
    table_candidates: list[str],
    metadata_context: dict[str, Any],
) -> tuple[str | None, str | None]:
    corrected, reason = _replace_disallowed_catalog(failed_sql, table_candidates)
    if reason:
        return corrected, reason

    corrected, reason = _replace_missing_column(failed_sql, error_message, metadata_context)
    if reason:
        return corrected, reason

    if "table" in error_message.lower() and any(token in error_message.lower() for token in ("not found", "does not exist", "not allowed")):
        corrected, reason = _replace_table_reference(failed_sql, table_candidates)
        if reason:
            return corrected, reason

    return None, None


def _llm_correction(
    *,
    question: str,
    intent_result: dict[str, Any],
    failed_sql: str,
    error_message: str,
    table_candidates: list[str],
    metadata_context: dict[str, Any],
    attempt_number: int,
) -> str | None:
    if not llm.llm_available():
        return None

    schema_context = _format_allowed_schema(metadata_context, table_candidates)
    prompt = f"""Fix this Trino SQL. Return SQL only. No markdown.

Question: {question}
Intent: {intent_result.get("intent")}
Correction attempt: {attempt_number} of {MAX_SQL_RETRY_ATTEMPTS}
Failed SQL:
{failed_sql}

Error:
{error_message}

Allowed Gold tables and columns:
{schema_context}

Rules:
- Use only {GOLD_PREFIX} tables listed above.
- Do not use postgresql.gold, analytics_test, Bronze, or Silver.
- Only SELECT or WITH.
- No INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE, CREATE, MERGE, CALL, GRANT, REVOKE.
- Trino-compatible SQL only.
"""

    content = llm.chat_completion(
        [{"role": "user", "content": prompt}],
        temperature=0,
        max_tokens=200,
    )
    return _clean_sql_response(content) or None


def correct_sql(
    *,
    question: str,
    intent_result: dict[str, Any],
    failed_sql: str,
    error_message: str,
    table_candidates: list[str],
    metadata_context: dict[str, Any],
    attempt_number: int,
) -> dict[str, Any]:
    if attempt_number > MAX_SQL_RETRY_ATTEMPTS:
        return {
            "corrected_sql": "",
            "correction_reason": f"Retry limit reached after {MAX_SQL_RETRY_ATTEMPTS} attempts.",
            "can_retry": False,
        }

    if not failed_sql.strip():
        return {
            "corrected_sql": "",
            "correction_reason": "No failed SQL to correct.",
            "can_retry": False,
        }

    if _contains_dangerous_keyword(failed_sql):
        return {
            "corrected_sql": "",
            "correction_reason": "SQL contains a blocked DDL/DML keyword and will not be retried.",
            "can_retry": False,
        }

    corrected_sql, reason = _heuristic_correction(
        failed_sql=failed_sql,
        error_message=error_message,
        table_candidates=table_candidates,
        metadata_context=metadata_context,
    )
    if corrected_sql:
        return {
            "corrected_sql": _clean_sql_response(corrected_sql),
            "correction_reason": reason,
            "can_retry": True,
        }

    corrected_sql = _llm_correction(
        question=question,
        intent_result=intent_result,
        failed_sql=failed_sql,
        error_message=error_message,
        table_candidates=table_candidates,
        metadata_context=metadata_context,
        attempt_number=attempt_number,
    )
    if not corrected_sql:
        return {
            "corrected_sql": "",
            "correction_reason": "No safe deterministic correction found and LLM correction was unavailable.",
            "can_retry": False,
        }

    return {
        "corrected_sql": corrected_sql,
        "correction_reason": "LLM generated a corrected Trino SQL using Gold metadata.",
        "can_retry": True,
    }
