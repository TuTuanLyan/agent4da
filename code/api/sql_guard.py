import re

GOLD_CATALOG = "iceberg_catalog"
GOLD_SCHEMA = "gold"

SUMMARY_TABLES = {
    "daily_event_summary",
    "daily_product_summary",
    "daily_category_summary",
    "daily_brand_summary",
}
DIM_TABLES = {
    "dim_time",
    "dim_product",
    "dim_user",
    "dim_session",
}
FACT_TABLES = {
    "fact_events",
    "fact_sales",
}
ALLOWED_TABLE_NAMES = SUMMARY_TABLES | DIM_TABLES | FACT_TABLES
ALLOWED_TABLE_SPECS = {
    f"{GOLD_CATALOG}.{GOLD_SCHEMA}.{table}": (GOLD_CATALOG, GOLD_SCHEMA, table)
    for table in sorted(ALLOWED_TABLE_NAMES)
}
ALLOWED_TABLES = set(ALLOWED_TABLE_SPECS)
ALLOWED_METADATA_TABLES = {
    f"{GOLD_CATALOG}.information_schema.tables",
    f"{GOLD_CATALOG}.information_schema.columns",
}
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
    for canonical_table, (catalog, schema, table) in ALLOWED_TABLE_SPECS.items():
        table_ref = re.compile(
            rf"\b(FROM|JOIN)\s+((?:{catalog}\s*\.\s*)?(?:{schema}\s*\.\s*)?{table})\b",
            flags=re.IGNORECASE,
        )
        sql = table_ref.sub(lambda match, replacement=canonical_table: f"{match.group(1)} {replacement}", sql)
    return sql


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


def _has_limit(sql: str) -> bool:
    return bool(re.search(r"\bLIMIT\b", sql, flags=re.IGNORECASE))


def _uses_fact_table(sql: str) -> bool:
    compact_sql = re.sub(r"\s+", "", sql).lower()
    fact_refs = {
        f"{GOLD_CATALOG}.{GOLD_SCHEMA}.{table}".lower()
        for table in FACT_TABLES
    }
    return any(table_ref in compact_sql for table_ref in fact_refs)


def _is_aggregate_query(sql: str) -> bool:
    return bool(
        re.search(r"\bCOUNT\s*\(", sql, flags=re.IGNORECASE)
        or re.search(r"\bGROUP\s+BY\b", sql, flags=re.IGNORECASE)
    )


def _validate_show_tables(sql: str) -> str | None:
    match = re.fullmatch(
        rf"SHOW\s+TABLES\s+FROM\s+{GOLD_CATALOG}\s*\.\s*{GOLD_SCHEMA}",
        sql,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return f"SHOW TABLES FROM {GOLD_CATALOG}.{GOLD_SCHEMA}"


def _validate_describe(sql: str) -> str | None:
    match = re.fullmatch(
        rf"(?:DESCRIBE|DESC)\s+{GOLD_CATALOG}\s*\.\s*{GOLD_SCHEMA}\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)",
        sql,
        flags=re.IGNORECASE,
    )
    if not match:
        return None

    table = match.group(1).lower()
    if table not in ALLOWED_TABLE_NAMES:
        raise ValueError(f"Table is not allowed: {table}")
    return f"DESCRIBE {GOLD_CATALOG}.{GOLD_SCHEMA}.{table}"


def _is_gold_schema_filter(sql: str) -> bool:
    return bool(
        re.search(
            r"\btable_schema\s*=\s*'gold'",
            sql,
            flags=re.IGNORECASE,
        )
    )


def _validate_metadata_select(sql: str) -> bool:
    compact_sql = re.sub(r"\s+", "", sql).lower()
    metadata_refs = {table.lower() for table in ALLOWED_METADATA_TABLES}
    if not any(table in compact_sql for table in metadata_refs):
        return False

    if not _is_gold_schema_filter(sql):
        raise ValueError("Metadata queries must filter table_schema = 'gold'")

    for table_name in re.findall(r"\btable_name\s*=\s*'([^']+)'", sql, flags=re.IGNORECASE):
        if table_name.lower() not in ALLOWED_TABLE_NAMES:
            raise ValueError(f"Table is not allowed: {table_name}")

    allowed_refs = {table.lower() for table in ALLOWED_METADATA_TABLES}
    allowed_refs.update(_extract_cte_names(sql))
    for table_ref in _find_table_refs(sql):
        normalized_ref = table_ref.lower()
        if normalized_ref not in allowed_refs:
            raise ValueError(f"Table is not allowed: {table_ref}")

    return True


def validate_sql(sql: str) -> str:
    sql = _remove_markdown_fences(sql)
    sql = sql.strip()
    if not sql:
        raise ValueError("SQL is empty")

    while sql.endswith(";"):
        sql = sql[:-1].rstrip()

    if ";" in sql:
        raise ValueError("SQL must contain a single SELECT/WITH statement")

    for keyword in DISALLOWED_KEYWORDS:
        if re.search(rf"\b{keyword}\b", sql, flags=re.IGNORECASE):
            raise ValueError(f"Disallowed SQL keyword: {keyword}")

    show_sql = _validate_show_tables(sql)
    if show_sql:
        return show_sql

    describe_sql = _validate_describe(sql)
    if describe_sql:
        return describe_sql

    if not re.match(r"^(SELECT|WITH)\b", sql, flags=re.IGNORECASE):
        raise ValueError("Only SELECT, WITH, SHOW TABLES, or DESCRIBE queries are allowed")

    sql = _normalize_table_refs(sql)
    compact_sql = re.sub(r"\s+", "", sql).lower()

    if _validate_metadata_select(sql):
        if not re.search(r"\bLIMIT\b", sql, flags=re.IGNORECASE):
            sql = f"{sql} LIMIT 100"
        return sql

    if not any(table in compact_sql for table in ALLOWED_TABLES):
        allowed = ", ".join(sorted(ALLOWED_TABLES))
        raise ValueError(f"SQL must query at least one allowed table: {allowed}")

    _reject_comma_joins(sql)

    if _uses_fact_table(sql) and not (_has_limit(sql) or _is_aggregate_query(sql)):
        raise ValueError("Queries against Gold fact tables must include LIMIT unless they are aggregate queries")

    allowed_refs = {table.lower() for table in ALLOWED_TABLES}
    allowed_refs.update(_extract_cte_names(sql))
    for table_ref in _find_table_refs(sql):
        normalized_ref = table_ref.lower()
        if normalized_ref not in allowed_refs:
            raise ValueError(f"Table is not allowed: {table_ref}")

    if not re.search(r"\bLIMIT\b", sql, flags=re.IGNORECASE) and not _is_aggregate_query(sql):
        sql = f"{sql} LIMIT 20"

    return sql
