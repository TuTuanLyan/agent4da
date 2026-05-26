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


def guard_sql_node(state):
    sql = (state.get("generated_sql") or "").strip()
    normalized = re.sub(r"\s+", " ", sql).strip()
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

    return {}
