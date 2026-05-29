from typing import Any

from fastapi import FastAPI
from pydantic import BaseModel, Field

from agent_graph import run_agent_graph

app = FastAPI(title="Agent4DA SQL API")


class AskRequest(BaseModel):
    session_id: str = Field(..., min_length=1)
    user_id: str | None = None
    question: str = Field(..., min_length=1)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/ask")
def ask(request: AskRequest) -> dict[str, Any]:
    return run_agent_graph(
        session_id=request.session_id,
        user_id=request.user_id,
        question=request.question,
    )
