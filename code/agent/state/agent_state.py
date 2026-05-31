from typing import Optional, TypedDict

class AgentState(TypedDict, total=False):

    user_question: str

    safety: Optional[dict]

    schema_context: str

    metadata_source: Optional[str]

    metadata_warning: Optional[str]

    prompt: str

    generated_sql: str

    sql_attempts: Optional[list]

    retry_count: int

    max_retries: int

    last_sql_error: Optional[str]

    sql_validation: Optional[dict]

    query_result: Optional[list]

    result_profile: Optional[dict]

    chart_spec: Optional[dict]

    insight_summary: Optional[str]

    insight_error: Optional[str]

    missing_info: Optional[dict]

    final_answer: Optional[dict]

    error: Optional[str]
