import logging
import os
import sys
import time
from pathlib import Path
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


def run_agent(question, max_retries=3):
    question = (question or "").strip()
    if not question:
        raise HTTPException(status_code=400, detail="Question must not be empty.")

    request_id = uuid4().hex[:8]
    start_time = time.perf_counter()

    logger.info("[%s] Question: %s", request_id, question)

    try:
        state = graph.invoke({
            "user_question": question,
            "retry_count": 0,
            "max_retries": max_retries,
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
        "question": question,
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
