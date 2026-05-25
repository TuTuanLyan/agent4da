
import os
from services.trino_service import connect_to_trino
from services.trino_service import execute_query_to_dicts

host = os.getenv("TRINO_HOST", "localhost")
port = int(os.getenv("TRINO_PORT", "8082"))
user = os.getenv("TRINO_USER", "agent4da")

connection = connect_to_trino(
    host=host,
    port=port,
    user=user,
    catalog="iceberg",
    schema="metadata"
)

def execute_sql_node(state):

    if state.get("error"):
        return state

    result = execute_query_to_dicts(
        connection,
        state["generated_sql"]
    )

    return {
        "query_result": result
    }