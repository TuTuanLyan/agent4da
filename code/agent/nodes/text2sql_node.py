import re
from services.llm_service import generate_sql

def clean_sql_query(generated_query: str) -> str:
    cleaned = re.sub(r'^\s*```(?:sql)?\s*', '', generated_query, flags=re.IGNORECASE)
    cleaned = re.sub(r'\s*```\s*$', '', cleaned)
    cleaned = cleaned.strip().rstrip(';')
    return cleaned.strip()


def generate_sql_node(state):

    if state.get("error"):
        return {}

    sql = generate_sql(state["prompt"])

    return {
        "generated_sql": clean_sql_query(sql)
    }
