import re

ALLOWED_TABLE = "postgresql.analytics_test.test_sales"
DISALLOWED_KEYWORDS = (
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


def _remove_markdown_fences(sql: str) -> str:
    sql = sql.strip()
    fence_match = re.fullmatch(r"```(?:sql)?\s*(.*?)\s*```", sql, flags=re.IGNORECASE | re.DOTALL)
    if fence_match:
        sql = fence_match.group(1)
    return sql.strip()


def _normalize_table_refs(sql: str) -> str:
    table_ref = re.compile(
        r"\b(FROM|JOIN)\s+((?:postgresql\s*\.\s*)?(?:analytics_test\s*\.\s*)?test_sales)\b",
        flags=re.IGNORECASE,
    )
    return table_ref.sub(lambda match: f"{match.group(1)} {ALLOWED_TABLE}", sql)


def _extract_cte_names(sql: str) -> set[str]:
    if not re.match(r"^\s*WITH\b", sql, flags=re.IGNORECASE):
        return set()
    return {
        match.group(1).lower()
        for match in re.finditer(r"(?:WITH|,)\s+([A-Za-z_][A-Za-z0-9_]*)\s+AS\s*\(", sql, flags=re.IGNORECASE)
    }


def _find_table_refs(sql: str) -> list[str]:
    refs = []
    for match in re.finditer(
        r"\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){0,2})\b",
        sql,
        flags=re.IGNORECASE,
    ):
        refs.append(match.group(1))
    return refs


def _reject_comma_joins(sql: str) -> None:
    for match in re.finditer(
        r"\bFROM\b(.*?)(?=\bWHERE\b|\bGROUP\s+BY\b|\bHAVING\b|\bORDER\s+BY\b|\bLIMIT\b|$)",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        if "," in match.group(1):
            raise ValueError("Comma joins are not allowed")


def validate_sql(sql: str) -> str:
    sql = _remove_markdown_fences(sql)
    sql = sql.strip()
    if not sql:
        raise ValueError("SQL is empty")

    while sql.endswith(";"):
        sql = sql[:-1].rstrip()

    if ";" in sql:
        raise ValueError("SQL must contain a single SELECT/WITH statement")

    if not re.match(r"^(SELECT|WITH)\b", sql, flags=re.IGNORECASE):
        raise ValueError("Only SELECT or WITH queries are allowed")

    for keyword in DISALLOWED_KEYWORDS:
        if re.search(rf"\b{keyword}\b", sql, flags=re.IGNORECASE):
            raise ValueError(f"Disallowed SQL keyword: {keyword}")

    sql = _normalize_table_refs(sql)
    compact_sql = re.sub(r"\s+", "", sql).lower()
    if ALLOWED_TABLE not in compact_sql:
        raise ValueError(f"SQL must query only {ALLOWED_TABLE}")

    _reject_comma_joins(sql)

    allowed_refs = {ALLOWED_TABLE.lower(), *_extract_cte_names(sql)}
    for table_ref in _find_table_refs(sql):
        normalized_ref = table_ref.lower()
        if normalized_ref not in allowed_refs:
            raise ValueError(f"Table is not allowed: {table_ref}")

    if not re.search(r"\bLIMIT\b", sql, flags=re.IGNORECASE) and not re.search(
        r"\bGROUP\s+BY\b", sql, flags=re.IGNORECASE
    ):
        sql = f"{sql} LIMIT 20"

    return sql
