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
SEMANTIC_TABLE_CATALOG = f"{GOLD_CATALOG}.metadata.semantic_table_catalog"
SEMANTIC_COLUMN_CATALOG = f"{GOLD_CATALOG}.metadata.semantic_column_catalog"
ALLOWED_METADATA_TABLES = {
    SEMANTIC_TABLE_CATALOG,
    SEMANTIC_COLUMN_CATALOG,
}
LEGACY_METADATA_TABLES = {
    f"{GOLD_CATALOG}.information_schema.tables",
    f"{GOLD_CATALOG}.information_schema.columns",
}
ALLOWED_SEMANTIC_METADATA_TABLES = {
    f"{GOLD_CATALOG}.metadata.semantic_table_catalog",
    f"{GOLD_CATALOG}.metadata.semantic_column_catalog",
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


def _metadata_tables_sql() -> str:
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


def _metadata_columns_sql(table_name: str | None = None) -> str:
    if table_name:
        qualified_table_name = f"{GOLD_SCHEMA}.{table_name}"
        return (
            "SELECT DISTINCT column_name, data_type "
            f"FROM {SEMANTIC_COLUMN_CATALOG} "
            "WHERE is_agent_visible = true "
            f"AND table_name IN ('{table_name}', '{qualified_table_name}') "
            "ORDER BY column_name"
        )
    table_expr = (
        f"CASE WHEN starts_with(table_name, '{GOLD_SCHEMA}.') "
        f"THEN substr(table_name, {len(GOLD_SCHEMA) + 2}) "
        "ELSE table_name END"
    )
    return (
        f"SELECT DISTINCT {table_expr} AS table_name, column_name, data_type "
        f"FROM {SEMANTIC_COLUMN_CATALOG} "
        "WHERE is_agent_visible = true "
        "ORDER BY table_name, column_name "
        "LIMIT 1000"
    )


def _validate_show_tables(sql: str) -> str | None:
    match = re.fullmatch(
        rf"SHOW\s+TABLES\s+FROM\s+{GOLD_CATALOG}\s*\.\s*{GOLD_SCHEMA}",
        sql,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return _metadata_tables_sql()


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
            rf"\btable_schema\s*=\s*'{re.escape(GOLD_SCHEMA)}'",
            sql,
            flags=re.IGNORECASE,
        )
    )


def _validate_metadata_select(sql: str) -> bool:
    compact_sql = re.sub(r"\s+", "", sql).lower()
    metadata_refs = {table.lower() for table in ALLOWED_METADATA_TABLES}
    semantic_refs = {table.lower() for table in ALLOWED_SEMANTIC_METADATA_TABLES}
    uses_information_schema = any(table in compact_sql for table in metadata_refs)
    uses_semantic_metadata = any(table in compact_sql for table in semantic_refs)
    if not uses_information_schema and not uses_semantic_metadata:
        return False

    if uses_information_schema and not _is_gold_schema_filter(sql):
        raise ValueError("Metadata queries must filter table_schema = 'gold'")

    for table_name in re.findall(r"\btable_name\s*=\s*'([^']+)'", sql, flags=re.IGNORECASE):
        normalized_table_name = table_name.lower()
        if normalized_table_name.startswith(f"{GOLD_SCHEMA}."):
            normalized_table_name = normalized_table_name.split(".", 1)[1]
        if normalized_table_name not in ALLOWED_TABLE_NAMES:
            raise ValueError(f"Table is not allowed: {table_name}")

    allowed_refs = {table.lower() for table in ALLOWED_METADATA_TABLES}
    allowed_refs.update(table.lower() for table in ALLOWED_SEMANTIC_METADATA_TABLES)
def _has_agent_visible_filter(sql: str) -> bool:
    return bool(re.search(r"\bis_agent_visible\s*=\s*true\b", sql, flags=re.IGNORECASE))


def _table_name_filters(sql: str) -> list[str]:
    return re.findall(r"\btable_name\s*=\s*'([^']+)'", sql, flags=re.IGNORECASE)


def _normalize_metadata_table_name(table_name: str) -> str:
    normalized = table_name.lower()
    schema_prefix = f"{GOLD_SCHEMA}."
    if normalized.startswith(schema_prefix):
        normalized = normalized[len(schema_prefix):]
    if normalized not in ALLOWED_TABLE_NAMES:
        raise ValueError(f"Table is not allowed: {table_name}")
    return normalized


def _validate_table_name_filters(sql: str) -> list[str]:
    table_names = _table_name_filters(sql)
    return [_normalize_metadata_table_name(table_name) for table_name in table_names]


def _validate_metadata_refs(sql: str, allowed_tables: set[str]) -> None:
    allowed_refs = {table.lower() for table in allowed_tables}
    allowed_refs.update(_extract_cte_names(sql))
    for table_ref in _find_table_refs(sql):
        normalized_ref = table_ref.lower()
        if normalized_ref not in allowed_refs:
            raise ValueError(f"Table is not allowed: {table_ref}")

    if uses_semantic_metadata and not re.search(
        r"\bis_agent_visible\s*=\s*(?:true|TRUE)\b",
        sql,
        flags=re.IGNORECASE,
    ):
        raise ValueError("Semantic metadata queries must filter is_agent_visible = true")

    return True

def _validate_metadata_select(sql: str) -> str | None:
    compact_sql = re.sub(r"\s+", "", sql).lower()

    legacy_refs = {table.lower() for table in LEGACY_METADATA_TABLES}
    if f"{GOLD_CATALOG}.information_schema.tables".lower() in compact_sql:
        if not _is_gold_schema_filter(sql):
            raise ValueError(f"Metadata queries must filter table_schema = '{GOLD_SCHEMA}'")
        _validate_table_name_filters(sql)
        _validate_metadata_refs(sql, LEGACY_METADATA_TABLES)
        return _metadata_tables_sql()

    if f"{GOLD_CATALOG}.information_schema.columns".lower() in compact_sql:
        if not _is_gold_schema_filter(sql):
            raise ValueError(f"Metadata queries must filter table_schema = '{GOLD_SCHEMA}'")
        table_names = _validate_table_name_filters(sql)
        _validate_metadata_refs(sql, LEGACY_METADATA_TABLES)
        return _metadata_columns_sql(table_names[0]) if len(table_names) == 1 else _metadata_columns_sql()

    metadata_refs = {table.lower() for table in ALLOWED_METADATA_TABLES}
    if not any(table in compact_sql for table in metadata_refs | legacy_refs):
        return None

    if not _has_agent_visible_filter(sql):
        raise ValueError("Semantic metadata queries must filter is_agent_visible = true")

    _validate_table_name_filters(sql)
    _validate_metadata_refs(sql, ALLOWED_METADATA_TABLES)

    if not re.search(r"\bLIMIT\b", sql, flags=re.IGNORECASE):
        sql = f"{sql} LIMIT 100"
    return sql


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

    metadata_sql = _validate_metadata_select(sql)
    if metadata_sql:
        return metadata_sql

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
