from typing import Optional, TypedDict

class AgentState(TypedDict, total=False):

    user_question: str

    request_id: Optional[str]

    session_id: Optional[str]

    user_id: Optional[str]

    app_context: Optional[dict]

    context_warning: Optional[str]

    safety: Optional[dict]

    schema_context: str

    metadata_source: Optional[str]

    metadata_warning: Optional[str]

    resolved_entities: Optional[list]

    entity_resolution_warning: Optional[str]

    prompt: str

    generated_sql: str

    sql_attempts: Optional[list]

    retry_count: int

    max_retries: int

    requery_count: int

    max_requery_rounds: int

    last_sql_error: Optional[str]

    sql_validation: Optional[dict]

    query_result: Optional[list]

    result_profile: Optional[dict]

    result_validation: Optional[dict]

    requery_requested: bool

    chart_spec: Optional[dict]

    chart_type_requested: Optional[str]

    insight_summary: Optional[str]

    insight_error: Optional[str]

    missing_info: Optional[dict]

    answer_kind: Optional[str]

    text_answer: Optional[str]

    stop_reason: Optional[str]

    stop_after_answerability: bool

    final_answer: Optional[dict]

    error: Optional[str]
