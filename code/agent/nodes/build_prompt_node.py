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

    prompt = f"""
        {SYSTEM_PROMPT}

        SCHEMA:
        {state["metadata_context"]}

        USER QUESTION:
        {state["user_question"]}
        """

    return {
        "prompt": prompt
    }