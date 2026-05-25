from typing import TypedDict, Optional, List, Dict, Any


class AgentState(TypedDict):
    user_question: str

    metadata_context: str

    prompt: str

    generated_sql: str

    query_result: Optional[List[Dict[str, Any]]]

    error: Optional[str]