from services.security_service import add_default_limit, validate_readonly_sql


def guard_sql_node(state):
    validation = validate_readonly_sql(state.get("generated_sql") or "")

    if not validation["allowed"]:
        return {
            "generated_sql": "",
            "sql_validation": validation,
            "error": validation["reason"],
        }

    limited_sql = add_default_limit(validation["sql"])
    validation = dict(validation)
    validation["sql"] = limited_sql

    return {
        "generated_sql": limited_sql,
        "sql_validation": validation,
        "error": None,
    }
