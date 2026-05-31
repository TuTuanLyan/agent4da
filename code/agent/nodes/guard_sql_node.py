import re


FORBIDDEN_KEYWORDS = [
    "INSERT",
    "UPDATE",
    "DELETE",
    "DROP",
    "ALTER",
    "TRUNCATE",
    "CREATE",
]

_LIMIT_PATTERN = re.compile(r"\blimit\s+\d+\b", flags=re.IGNORECASE)
_FACT_TABLE_PATTERN = re.compile(r"\bfact_[a-z_]+\b", flags=re.IGNORECASE)


def _allowed_tables_from_context(schema_context):
    allowed = set()
    for line in (schema_context or "").splitlines():
        line = line.strip()
        if not line.lower().startswith("table:"):
            continue
        name = line.split(":", 1)[1].strip()
        if not name:
            continue
        allowed.add(name.lower())
        parts = name.split(".")
        if parts:
            allowed.add(parts[-1].lower())
        if len(parts) == 2:
            allowed.add(f"iceberg.{name}".lower())
    return allowed


def _referenced_tables(sql):
    try:
        from sqlglot import exp, parse_one
    except ImportError:
        return set()

    parsed = parse_one(sql, read="trino")
    refs = set()
    for table in parsed.find_all(exp.Table):
        parts = []
        catalog = getattr(table, "catalog", None)
        db = getattr(table, "db", None)
        name = getattr(table, "name", None)
        if catalog:
            parts.append(catalog)
        if db:
            parts.append(db)
        if name:
            parts.append(name)
        if parts:
            refs.add(".".join(parts).lower())
            refs.add(parts[-1].lower())
            if len(parts) >= 2:
                refs.add(".".join(parts[-2:]).lower())
    return refs


def guard_sql_node(state):
    if state.get("error"):
        return {}

    sql = (state.get("generated_sql") or "").strip()
    stripped = sql.rstrip(";").strip()
    normalized = re.sub(r"\s+", " ", stripped).strip()
    upper_sql = normalized.upper()

    if not upper_sql.startswith("SELECT"):
        return {
            "error": "Only SELECT SQL is allowed.",
        }

    if re.search(r";\s*\S", sql):
        return {
            "error": "Only one SQL statement is allowed.",
        }

    for keyword in FORBIDDEN_KEYWORDS:
        if re.search(rf"\b{keyword}\b", upper_sql):
            return {
                "error": f"Forbidden SQL keyword: {keyword}",
            }

    try:
        allowed_tables = _allowed_tables_from_context(state.get("schema_context"))
        referenced_tables = _referenced_tables(stripped)
    except Exception:
        return {
            "error": "SQL could not be parsed for safety validation.",
        }

    unknown = sorted(
        ref for ref in referenced_tables
        if ref not in allowed_tables and not ref.startswith("system.")
    )
    if allowed_tables and unknown:
        return {
            "error": "SQL references a table outside the visible semantic catalog.",
        }

    if _FACT_TABLE_PATTERN.search(stripped) and not _LIMIT_PATTERN.search(stripped):
        return {"generated_sql": stripped + "\nLIMIT 10000"}

    return {"generated_sql": stripped}
