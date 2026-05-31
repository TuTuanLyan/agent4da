from services.trino_service import execute_query_to_dicts, get_trino_connection


def execute_sql_node(state):
    if state.get("error"):
        return {}

    try:
        result = execute_query_to_dicts(
            get_trino_connection(),
            state["generated_sql"],
            raise_on_error=True,
        )
    except Exception as exc:
        retry_count = int(state.get("retry_count") or 0) + 1
        message = f"Trino query failed: {type(exc).__name__}: {exc}"
        return {
            "query_result": [],
            "retry_count": retry_count,
            "last_sql_error": message,
            "error": message,
        }

    return {
        "query_result": result,
        "last_sql_error": None,
        "error": None,
    }
