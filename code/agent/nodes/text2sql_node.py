from services.llm_service import generate_sql
from services.security_service import clean_sql


def generate_sql_node(state):
    sql = clean_sql(generate_sql(state["prompt"]))
    attempts = list(state.get("sql_attempts") or [])
    attempts.append(sql)

    return {
        "generated_sql": sql,
        "sql_attempts": attempts,
        "requery_requested": False,
        "error": None,
    }
