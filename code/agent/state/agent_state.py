from typing import TypedDict, Optional

class AgentState(TypedDict, total=False):

    user_question: str

    schema_context: str

    prompt: str

    generated_sql: str

    query_result: Optional[list]

    error: Optional[str]
