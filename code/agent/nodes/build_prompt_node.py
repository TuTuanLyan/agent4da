from nodes.filter_schema_node import build_schema_context


SYSTEM_PROMPT = """
You are a Trino SQL expert.

Generate ONLY SQL.

Rules:
- Use only provided tables
- Use Trino SQL syntax
- Do not explain
- Output SQL only
"""


def build_prompt_node(state):
    metadata = state.get("filtered_metadata") or state["full_metadata"]
    schema_context = build_schema_context(metadata)

    prompt = f"""
        {SYSTEM_PROMPT}

        SCHEMA:
        {schema_context}

        USER QUESTION:
        {state["user_question"]}
        """

    return {
        "prompt": prompt
    }
