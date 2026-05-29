from typing import Any, List, Optional, TypedDict


class AgentState(TypedDict, total=False):

    user_question: str

    schema_context: str

    prompt: str

    generated_sql: str

    query_result: Optional[list]

    error: Optional[str]

    # Phase 3 additions ------------------------------------------------------

    # Optional natural-language summary of the result, produced by the
    # summarize node when AGENT_SUMMARIZE is truthy.
    summary: Optional[str]

    # Small list of {label, value} entries pulled from the result rows that
    # the UI renders as KPI cards on the Answer tab.
    key_numbers: Optional[List[Any]]

    # Allow the API layer to opt in/out of summarize per request.
    summarize: Optional[bool]

    # The web app passes its own run_id so the API can correlate the run
    # with its app.query_runs row. Optional for the CLI debug script.
    run_id: Optional[str]
