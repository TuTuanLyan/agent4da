import logging
import os
import sys
import time
from pathlib import Path
from typing import Optional
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CODE_DIR = PROJECT_ROOT / "code"
AGENT_DIR = CODE_DIR / "agent"
LOG_FILE = PROJECT_ROOT / "log" / "agent.log"


def load_local_env_files():
    try:
        from dotenv import load_dotenv
    except Exception:
        return

    for file_name in ["endpoint.env", "groq.env", "postgre.env", "iceberg.env"]:
        env_file = PROJECT_ROOT / "envs" / file_name
        if env_file.exists():
            load_dotenv(env_file, override=False)


load_local_env_files()

for path in [CODE_DIR, AGENT_DIR]:
    path_text = str(path)
    if path_text not in sys.path:
        sys.path.insert(0, path_text)

from graph.sql_graph import graph
from services.metadata_service import (
    build_schema_context,
    load_semantic_metadata,
    summarize_metadata,
)
from services.security_service import detect_question_risk, validate_readonly_sql
from services.context_service import (
    ContextStoreError,
    build_compact_context,
    context_health,
    create_session,
    ensure_context_tables,
    get_latest_context,
    get_messages,
    get_or_create_session,
    get_query_logs,
    save_checkpoint,
    save_context_snapshot,
    save_message,
    save_query_log,
)
from utils.logger import get_logger, setup_logging


setup_logging(
    log_file=LOG_FILE,
    log_level=logging.INFO,
    console=False,
    reset_file=True,
    logger_name="agent",
)
logger = get_logger("agent.api")
logger.info("Agent API logging initialized. log_file=%s reset_file=True", LOG_FILE)


class AskRequest(BaseModel):
    question: str = Field(
        ...,
        min_length=1,
        examples=[
            "Doanh thu theo ngay trong thang 1 nam 2020"
        ],
    )
    max_retries: int = Field(
        3,
        ge=1,
        le=3,
        description="Số lần thử SQL tối đa khi Trino báo lỗi.",
    )


class AgentAskRequest(BaseModel):
    question: str = Field(..., min_length=1)
    session_id: Optional[str] = Field(
        None,
        description="Session chat hiện có. Nếu bỏ trống API sẽ tạo session mới.",
    )
    user_id: str = Field(
        "default",
        description="Tạm dùng cho demo; auth thật sẽ do app backend xử lý.",
    )
    app_context: Optional[dict] = Field(
        None,
        description="Context compact truyền trực tiếp. Nếu có session_id, DB context được ưu tiên.",
    )
    max_sql_retries: int = Field(3, ge=1, le=3)
    max_requery_rounds: int = Field(1, ge=0, le=2)
    chart_type: str = Field(
        "auto",
        description="FE có thể gửi auto/bar/line/pie/table/scatter. Backend hiện trả suggestion đơn giản.",
    )


class SessionCreateRequest(BaseModel):
    user_id: str = Field("default")
    session_name: Optional[str] = Field(None)


class QuestionGuardRequest(BaseModel):
    question: str = Field(..., min_length=1)


class SqlGuardRequest(BaseModel):
    sql: str = Field(..., min_length=1)


app = FastAPI(
    title="Agent4DA API",
    description=(
        "API test nhanh cho Agent Text-to-SQL chỉ đọc. "
        "Dùng /docs để thử bằng Swagger UI."
    ),
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return RedirectResponse(url="/docs")


@app.get("/health")
def health():
    return {
        "status": "ok",
        "readonly": True,
        "api_docs": "http://localhost:8001/docs",
        "log_file": str(LOG_FILE),
        "groq_api_key_configured": bool(os.getenv("GROQ_API_KEY")),
    }


@app.get("/api/v1/health")
def health_v1():
    return health()


@app.post("/api/v1/guard/question")
def guard_question(request: QuestionGuardRequest):
    return detect_question_risk(request.question)


@app.post("/api/v1/guard/sql")
def guard_sql(request: SqlGuardRequest):
    return validate_readonly_sql(request.sql)


@app.get("/api/v1/metadata")
def get_metadata():
    metadata = load_semantic_metadata()
    return summarize_metadata(metadata)


@app.get("/api/v1/schema-context")
def get_schema_context():
    metadata = load_semantic_metadata()
    return {
        "source": metadata.get("source"),
        "warning": metadata.get("warning"),
        "schema_context": build_schema_context(metadata),
    }


@app.get("/api/v1/agent/context/health")
def get_agent_context_health():
    return context_health()


@app.post("/api/v1/agent/context/init")
def init_agent_context():
    try:
        return ensure_context_tables(force=True)
    except ContextStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.post("/api/v1/agent/sessions")
def create_agent_session(request: SessionCreateRequest):
    try:
        session = create_session(
            user_id=request.user_id,
            session_name=request.session_name,
        )
        context = get_latest_context(session["session_id"])
    except ContextStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return {
        "session": session,
        "context": context,
    }


@app.get("/api/v1/agent/sessions/{session_id}/context")
def get_agent_session_context(session_id: str):
    try:
        session = get_or_create_session(session_id=session_id)
        context = get_latest_context(session_id)
    except ContextStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return {
        "session": session,
        "context": context,
    }


@app.get("/api/v1/agent/sessions/{session_id}/messages")
def get_agent_session_messages(session_id: str, limit: int = 50):
    try:
        return {
            "session_id": session_id,
            "messages": get_messages(session_id, limit=limit),
        }
    except ContextStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/api/v1/agent/sessions/{session_id}/queries")
def get_agent_session_queries(session_id: str, limit: int = 50):
    try:
        return {
            "session_id": session_id,
            "queries": get_query_logs(session_id, limit=limit),
        }
    except ContextStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


def run_agent(
    question,
    max_retries=3,
    request_id=None,
    session_id=None,
    user_id=None,
    app_context=None,
    context_warning=None,
    max_requery_rounds=1,
    chart_type="auto",
):
    question = (question or "").strip()
    if not question:
        raise HTTPException(status_code=400, detail="Question must not be empty.")

    request_id = request_id or uuid4().hex[:8]
    start_time = time.perf_counter()

    logger.info("[%s] Question: %s", request_id, question)

    try:
        state = graph.invoke({
            "request_id": request_id,
            "session_id": session_id,
            "user_id": user_id,
            "user_question": question,
            "app_context": app_context or {},
            "context_warning": context_warning,
            "retry_count": 0,
            "max_retries": max_retries,
            "requery_count": 0,
            "max_requery_rounds": max_requery_rounds,
            "chart_type_requested": chart_type,
        })
    except Exception as exc:
        elapsed = time.perf_counter() - start_time
        logger.exception("[%s] Agent failed after %.2fs", request_id, elapsed)
        raise HTTPException(
            status_code=500,
            detail=f"Agent failed: {type(exc).__name__}: {exc}",
        ) from exc

    final_answer = state.get("final_answer") or {
        "status": "error",
        "request_id": request_id,
        "session_id": session_id,
        "user_id": user_id,
        "answer_kind": "error",
        "question": question,
        "text_answer": state.get("error") or "Agent did not return final_answer.",
        "sql": state.get("generated_sql") or "",
        "readonly": True,
        "result": {
            "row_count": 0,
            "columns": [],
            "rows": state.get("query_result") or [],
            "missing_info": {
                "has_missing_info": True,
                "items": ["Agent không tạo được final_answer."],
                "can_requery": True,
                "notes": state.get("error") or "",
            },
        },
        "analysis": {
            "insight_summary": state.get("insight_summary"),
            "insight_error": state.get("insight_error"),
            "result_profile": state.get("result_profile"),
            "missing_info": state.get("missing_info"),
        },
        "visualization": {
            "chart_spec": state.get("chart_spec"),
        },
        "chart_suggestion": {
            "chart_type": "none",
            "x": None,
            "y": None,
            "reason": "Agent did not return final_answer.",
        },
        "blocks": [
            {
                "type": "error",
                "title": "Lỗi",
                "content": state.get("error") or "Agent did not return final_answer.",
            }
        ],
        "error": state.get("error") or "Agent did not return final_answer.",
    }

    elapsed = time.perf_counter() - start_time
    final_answer.setdefault("meta", {})
    final_answer["meta"].update({
        "execution_time_ms": int(elapsed * 1000),
        "max_sql_retries": max_retries,
        "max_requery_rounds": max_requery_rounds,
    })
    result = final_answer.get("result") or {}
    chart_spec = (final_answer.get("visualization") or {}).get("chart_spec") or {}

    logger.info(
        "[%s] Done status=%s rows=%s chart=%s elapsed=%.2fs",
        request_id,
        final_answer.get("status"),
        result.get("row_count"),
        chart_spec.get("type"),
        elapsed,
    )

    if final_answer.get("error"):
        logger.error("[%s] Error: %s", request_id, final_answer.get("error"))

    logger.info("[%s] SQL: %s", request_id, final_answer.get("sql") or "")

    return final_answer


def run_agent_with_persistent_context(request: AgentAskRequest):
    request_id = uuid4().hex[:8]
    session = None
    session_id = request.session_id
    context_warning = None
    app_context = request.app_context or {}

    try:
        session = get_or_create_session(
            session_id=request.session_id,
            user_id=request.user_id,
        )
        session_id = session["session_id"]
        if request.session_id or not request.app_context:
            app_context = get_latest_context(session_id)
        save_message(
            session_id,
            "user",
            request.question,
            payload={"request_id": request_id},
        )
    except Exception as exc:
        context_warning = (
            "Không lưu/load được app_context từ PostgreSQL, request này chạy tạm "
            f"ở chế độ stateless. Lỗi: {type(exc).__name__}: {exc}"
        )
        logger.warning("[%s] Context unavailable: %s", request_id, context_warning)
        session_id = session_id or uuid4().hex

    final_answer = run_agent(
        request.question,
        max_retries=request.max_sql_retries,
        request_id=request_id,
        session_id=session_id,
        user_id=request.user_id,
        app_context=app_context,
        context_warning=context_warning,
        max_requery_rounds=request.max_requery_rounds,
        chart_type=request.chart_type,
    )

    compact_context = build_compact_context(app_context, final_answer)
    final_answer.setdefault("context", {})
    final_answer["context"].update({
        "compact": compact_context,
        "compact_summary": compact_context.get("conversation_summary") or "",
        "updated_at": compact_context.get("updated_at"),
        "warning": context_warning,
    })

    if context_warning:
        return final_answer

    try:
        save_message(
            session_id,
            "assistant",
            final_answer.get("text_answer") or "",
            payload=final_answer,
        )
        save_context_snapshot(session_id, compact_context)
        save_checkpoint(
            session_id,
            {
                "request_id": request_id,
                "final_answer": final_answer,
                "compact_context": compact_context,
            },
        )
        result = final_answer.get("result") or {}
        meta = final_answer.get("meta") or {}
        save_query_log(
            request_id=request_id,
            session_id=session_id,
            question=request.question,
            sql=final_answer.get("sql") or "",
            status=final_answer.get("status") or "unknown",
            execution_time_ms=meta.get("execution_time_ms") or 0,
            row_count=result.get("row_count") or 0,
            error_message=final_answer.get("error") or "",
        )
    except Exception as exc:
        warning = (
            "Agent đã trả lời nhưng không lưu được context/log sau request. "
            f"Lỗi: {type(exc).__name__}: {exc}"
        )
        final_answer["context"]["warning"] = warning
        logger.warning("[%s] Failed to persist context: %s", request_id, warning)

    return final_answer


@app.post("/api/v1/agent/ask")
def ask_agent_with_context(request: AgentAskRequest):
    return run_agent_with_persistent_context(request)


@app.post("/ask")
def ask_agent(request: AskRequest):
    return run_agent(request.question, request.max_retries)


@app.post("/api/v1/ask")
def ask_agent_v1(request: AskRequest):
    return run_agent(request.question, request.max_retries)


if __name__ == "__main__":
    import uvicorn

    host = os.getenv("AGENT_API_HOST", "0.0.0.0")
    port = int(os.getenv("AGENT_API_PORT", "8001"))

    uvicorn.run(app, host=host, port=port)
