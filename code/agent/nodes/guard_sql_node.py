FORBIDDEN_KEYWORDS = [
    "DELETE",
    "DROP",
    "TRUNCATE",
    "UPDATE",
    "INSERT",
    "ALTER",
]


def guard_sql_node(state):

    sql = state["generated_sql"].upper()

    for keyword in FORBIDDEN_KEYWORDS:
        if keyword in sql:
            return {
                "error": f"Forbidden SQL keyword: {keyword}"
            }

    return {}