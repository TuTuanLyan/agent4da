import time
from typing import Any

from fastapi import FastAPI
from pydantic import BaseModel, Field

from db_context import ensure_session, log_query
from llm_sql import generate_sql
from sql_guard import validate_sql
from trino_client import execute_query

app = FastAPI(title="Agent4DA SQL API")


class AskRequest(BaseModel):
    session_id: str = Field(..., min_length=1)
    user_id: str | None = None
    question: str = Field(..., min_length=1)


def _elapsed_ms(start: float) -> int:
    return int((time.perf_counter() - start) * 1000)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/ask")
def ask(request: AskRequest) -> dict[str, Any]:
    start = time.perf_counter()
    generated_sql = ""

    try:
        ensure_session(request.session_id, request.user_id)

        generated_sql = generate_sql(request.question)
        generated_sql = validate_sql(generated_sql)

        columns, rows = execute_query(generated_sql)
        execution_time_ms = _elapsed_ms(start)
        log_query(
            request.session_id,
            request.question,
            generated_sql,
            "success",
            execution_time_ms,
        )

        return {
            "session_id": request.session_id,
            "question": request.question,
            "generated_sql": generated_sql,
            "columns": columns,
            "rows": rows,
            "execution_time_ms": execution_time_ms,
            "status": "success",
        }
    except Exception as exc:
        execution_time_ms = _elapsed_ms(start)
        error_message = str(exc)
        try:
            log_query(
                request.session_id,
                request.question,
                generated_sql,
                "error",
                execution_time_ms,
                error_message,
            )
        except Exception as log_exc:
            error_message = f"{error_message}; failed to write query log: {log_exc}"

        return {
            "session_id": request.session_id,
            "question": request.question,
            "generated_sql": generated_sql,
            "rows": [],
            "status": "error",
            "error_message": error_message,
        }
