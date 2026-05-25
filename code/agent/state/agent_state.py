from typing import TypedDict, Optional

class AgentState(TypedDict, total=False):

    user_question: str

    full_metadata: dict

    filtered_metadata: dict

    prompt: str

    generated_sql: str

    query_result: Optional[list]

    error: Optional[str]
