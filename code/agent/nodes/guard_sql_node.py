from services.security_service import validate_readonly_sql


def guard_sql_node(state):
    validation = validate_readonly_sql(state.get("generated_sql") or "")

    if not validation["allowed"]:
        return {
            "generated_sql": "",
            "sql_validation": validation,
            "error": validation["reason"],
        }

    return {
        "generated_sql": validation["sql"],
        "sql_validation": validation,
        "error": None,
    }
