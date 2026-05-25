from services.trino_service import execute_query_to_dicts, get_trino_connection

def execute_sql_node(state):

    if state.get("error"):
        return state

    result = execute_query_to_dicts(
        get_trino_connection(),
        state["generated_sql"]
    )

    return {
        "query_result": result
    }
