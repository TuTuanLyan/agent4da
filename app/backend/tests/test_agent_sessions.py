"""Focused tests for the chat-session APIs and service helpers.

No live Postgres in the sandbox, so we run the real routers/services/models over
SQLite with small @compiles shims (UUID -> CHAR(36), JSONB -> JSON, CITEXT ->
TEXT), a uuid string adapter, and schema_translate_map={"app": None}. Foreign
keys (incl. ON DELETE SET NULL) are enforced via PRAGMA foreign_keys=ON.

Run from app/backend:  python -m pytest tests/test_agent_sessions.py
"""

from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.dialects.postgresql import CITEXT, JSONB, UUID as PGUUID
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool


# --- SQLite shims for the postgres-specific column types --------------------

@compiles(PGUUID, "sqlite")
def _compile_uuid(element, compiler, **kw):  # noqa: ANN001
    return "CHAR(36)"


@compiles(JSONB, "sqlite")
def _compile_jsonb(element, compiler, **kw):  # noqa: ANN001
    return "JSON"


@compiles(CITEXT, "sqlite")
def _compile_citext(element, compiler, **kw):  # noqa: ANN001
    return "TEXT"


sqlite3.register_adapter(uuid.UUID, str)


# --- App imports (resolve once SQLAlchemy is configured) --------------------

from db.base import Base, get_db  # noqa: E402
from db.models import AgentFeedback, AgentSuggestionEvent, ChatSession, QueryRun, User  # noqa: E402
from auth.deps import current_user  # noqa: E402
from agent.contextual_learning import rank_contextual_suggestions  # noqa: E402
from agent import router as agent_router_module  # noqa: E402
from agent.router import router as agent_router  # noqa: E402
from agent.service import session_title_from_question, update_session_after_run  # noqa: E402


# --- Test harness -----------------------------------------------------------

_CTX = SimpleNamespace(user=None)


@pytest.fixture()
def client():
    base_engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(base_engine, "connect")
    def _fk_on(dbapi_conn, _rec):  # noqa: ANN001
        dbapi_conn.execute("PRAGMA foreign_keys=ON")

    engine = base_engine.execution_options(schema_translate_map={"app": None})
    Base.metadata.create_all(engine)
    TestingSession = sessionmaker(bind=engine, expire_on_commit=False, future=True)

    # Seed two users.
    db = TestingSession()
    user_a = User(id=uuid.uuid4(), email="a@example.com", password_hash="x")
    user_b = User(id=uuid.uuid4(), email="b@example.com", password_hash="x")
    db.add_all([user_a, user_b])
    db.commit()
    a_id, b_id = user_a.id, user_b.id
    db.close()

    _CTX.user = SimpleNamespace(id=a_id)

    def override_get_db():
        s = TestingSession()
        try:
            yield s
        finally:
            s.close()

    def override_current_user():
        return _CTX.user

    app = FastAPI()
    app.include_router(agent_router)
    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[current_user] = override_current_user

    tc = TestClient(app)
    tc.SessionLocal = TestingSession  # type: ignore[attr-defined]
    tc.user_a = a_id  # type: ignore[attr-defined]
    tc.user_b = b_id  # type: ignore[attr-defined]
    tc.as_user = lambda uid: setattr(_CTX, "user", SimpleNamespace(id=uid))  # type: ignore[attr-defined]
    yield tc


def _insert_run(SessionLocal, *, user_id, session_id, turn_index, question, status="success"):
    s = SessionLocal()
    try:
        run = QueryRun(
            id=uuid.uuid4(),
            user_id=user_id,
            session_id=session_id,
            turn_index=turn_index,
            question=question,
            status=status,
            agent_engine="legacy",
        )
        s.add(run)
        s.commit()
        return run.id
    finally:
        s.close()


# --- Tests ------------------------------------------------------------------

def test_session_title_truncation():
    assert session_title_from_question("  Doanh thu  theo  ngay ") == "Doanh thu theo ngay"
    assert session_title_from_question("") == "Cuoc tro chuyen moi"
    long_q = "a" * 200
    title = session_title_from_question(long_q)
    assert len(title) <= 80 and title.endswith("...")


def test_create_and_list_sessions(client):
    created = client.post("/agent/sessions")
    assert created.status_code == 201
    sid = created.json()["id"]
    assert created.json()["is_pinned"] is False
    assert created.json()["pinned_at"] is None

    listed = client.get("/agent/sessions")
    assert listed.status_code == 200
    rows = listed.json()
    assert any(r["id"] == sid for r in rows)
    row = next(r for r in rows if r["id"] == sid)
    assert row["run_count"] == 0
    assert row["last_question"] is None


def test_rename_and_clear_session_title(client):
    sid = client.post("/agent/sessions").json()["id"]

    renamed = client.patch(f"/agent/sessions/{sid}", json={"title": "  Revenue room  "})
    assert renamed.status_code == 200, renamed.text
    assert renamed.json()["title"] == "Revenue room"

    cleared = client.patch(f"/agent/sessions/{sid}", json={"title": ""})
    assert cleared.status_code == 200, cleared.text
    assert cleared.json()["title"] is None


def test_pin_session_and_sort_pinned_first(client):
    first = client.post("/agent/sessions").json()["id"]
    second = client.post("/agent/sessions").json()["id"]

    pinned = client.patch(f"/agent/sessions/{first}", json={"is_pinned": True})
    assert pinned.status_code == 200, pinned.text
    assert pinned.json()["is_pinned"] is True
    assert pinned.json()["pinned_at"] is not None

    rows = client.get("/agent/sessions").json()
    assert rows[0]["id"] == first
    assert rows[0]["is_pinned"] is True
    assert any(row["id"] == second for row in rows[1:])

    unpinned = client.patch(f"/agent/sessions/{first}", json={"is_pinned": False})
    assert unpinned.status_code == 200, unpinned.text
    assert unpinned.json()["is_pinned"] is False
    assert unpinned.json()["pinned_at"] is None


def test_foreign_session_update_is_404(client):
    sid = client.post("/agent/sessions").json()["id"]
    client.as_user(client.user_b)
    assert client.patch(f"/agent/sessions/{sid}", json={"title": "nope"}).status_code == 404
    assert client.patch(f"/agent/sessions/{sid}", json={"is_pinned": True}).status_code == 404


def test_session_runs_oldest_first(client):
    sid = client.post("/agent/sessions").json()["id"]
    _insert_run(client.SessionLocal, user_id=client.user_a, session_id=uuid.UUID(sid), turn_index=1, question="Q1")
    _insert_run(client.SessionLocal, user_id=client.user_a, session_id=uuid.UUID(sid), turn_index=2, question="Q2")

    resp = client.get(f"/agent/sessions/{sid}/runs")
    assert resp.status_code == 200
    runs = resp.json()
    assert [r["question"] for r in runs] == ["Q1", "Q2"]
    # New response fields are present.
    assert runs[0]["agent_engine"] == "legacy"
    assert runs[0]["session_id"] == sid
    assert runs[0]["turn_index"] == 1
    assert "insights" in runs[0]


def test_foreign_session_is_404(client):
    sid = client.post("/agent/sessions").json()["id"]
    # Switch to user B; the session belongs to user A.
    client.as_user(client.user_b)
    assert client.get(f"/agent/sessions/{sid}/runs").status_code == 404
    assert client.delete(f"/agent/sessions/{sid}").status_code == 404


def test_delete_session_keeps_runs(client):
    sid = client.post("/agent/sessions").json()["id"]
    run_id = _insert_run(
        client.SessionLocal, user_id=client.user_a, session_id=uuid.UUID(sid), turn_index=1, question="Keep me"
    )

    assert client.delete(f"/agent/sessions/{sid}").status_code == 204
    # Session gone from the list.
    assert all(r["id"] != sid for r in client.get("/agent/sessions").json())

    # Run still exists, detached from the (now deleted) session.
    s = client.SessionLocal()
    try:
        run = s.get(QueryRun, run_id)
        assert run is not None
        assert run.session_id is None
    finally:
        s.close()


def test_auto_title_from_first_question(client):
    sid = client.post("/agent/sessions").json()["id"]
    s = client.SessionLocal()
    try:
        update_session_after_run(s, uuid.UUID(sid), "Top 5 brands by revenue last month")
        s.commit()
        chat = s.get(ChatSession, uuid.UUID(sid))
        assert chat.title == "Top 5 brands by revenue last month"
        # A second run must not overwrite the established title.
        update_session_after_run(s, uuid.UUID(sid), "follow up question")
        s.commit()
        s.refresh(chat)
        assert chat.title == "Top 5 brands by revenue last month"
    finally:
        s.close()


def test_feedback_persists_for_owned_run(client):
    sid = client.post("/agent/sessions").json()["id"]
    run_id = _insert_run(
        client.SessionLocal,
        user_id=client.user_a,
        session_id=uuid.UUID(sid),
        turn_index=1,
        question="Ambiguous question",
    )
    payload = {
        "run_id": str(run_id),
        "session_id": sid,
        "feedback_type": "suggestion_click",
        "selected_suggestion": {
            "label": "Theo brand",
            "question": "Top 5 brand theo doanh thu là gì?",
            "reason": "Brand có summary table riêng.",
            "intent": "ranking",
            "confidence": "high",
        },
    }

    resp = client.post("/agent/feedback", json=payload)
    assert resp.status_code == 201, resp.text

    s = client.SessionLocal()
    try:
        rows = s.query(AgentFeedback).all()
        assert len(rows) == 1
        assert rows[0].feedback_type == "suggestion_click"
        assert rows[0].selected_suggestion["intent"] == "ranking"
    finally:
        s.close()


def test_contextual_learning_ranks_clicked_suggestion(client):
    selected = {
        "label": "Theo category",
        "question": "Top 5 category theo doanh thu là gì?",
        "reason": "Category đã được chọn trong context tương tự.",
        "intent": "ranking",
        "confidence": "medium",
    }
    other = {
        "label": "Theo sản phẩm",
        "question": "Top 10 sản phẩm theo doanh thu là gì?",
        "reason": "Product drill-down.",
        "intent": "ranking",
        "confidence": "high",
    }

    s = client.SessionLocal()
    try:
        s.add(
            AgentFeedback(
                user_id=client.user_a,
                feedback_type="suggestion_click",
                selected_suggestion=selected,
            )
        )
        s.add(
            AgentSuggestionEvent(
                user_id=client.user_a,
                input_question="top gì tốt nhất",
                intent="ranking",
                suggestions_generated=[selected, other],
                selected_suggestion=selected,
                result_status="success",
            )
        )
        s.commit()

        state = {
            "agent_trace": {
                "intent": "ranking",
                "validation_notes": [],
                "clarification_suggestions": [other, selected],
            },
            "clarification_suggestions": [other, selected],
        }
        rank_contextual_suggestions(s, client.user_a, state)

        assert state["clarification_suggestions"][0]["question"] == selected["question"]
        assert "Contextual suggestion ranking" in state["agent_trace"]["validation_notes"][0]
    finally:
        s.close()


@pytest.mark.asyncio
async def test_stream_error_path_emits_terminal_result(monkeypatch):
    run_id = uuid.uuid4()
    session_id = uuid.uuid4()

    class Request:
        async def is_disconnected(self):
            return False

    async def fail_run(*args, **kwargs):  # noqa: ANN002, ANN003
        raise RuntimeError("boom")

    def persist_run(**kwargs):  # noqa: ANN003
        state = kwargs["state"]
        return SimpleNamespace(
            id=kwargs["run_id"],
            generated_sql=None,
            guard_status=kwargs["guard_status"],
            columns=[],
            result_json=[],
            row_count=0,
            error=state.get("error"),
            latency_ms=1,
            summary_text=None,
            insights=[],
            key_numbers=[],
            chart_suggestion=None,
            chart_type=None,
            chart_payload=None,
            chart_data=None,
            retry_count=0,
            model_used=None,
            agent_trace=None,
            agent_engine="legacy",
            status=kwargs["status"],
            session_id=kwargs["session_id"],
            turn_index=kwargs["turn_index"],
            created_at=datetime.now(timezone.utc),
        )

    monkeypatch.setattr(agent_router_module, "selected_agent_engine", lambda: "legacy")
    monkeypatch.setattr(agent_router_module, "_stream_context", lambda *args, **kwargs: ("question", 1))
    monkeypatch.setattr(agent_router_module, "run_agent_state", fail_run)
    monkeypatch.setattr(agent_router_module, "_persist_run_sync", persist_run)
    monkeypatch.setattr(uuid, "uuid4", lambda: run_id)

    chunks = [
        chunk
        async for chunk in agent_router_module._stream_run(
            Request(),
            user_id=str(uuid.uuid4()),
            question="question",
            summarize=None,
            chart_type_hint=None,
            session_id=session_id,
        )
    ]

    assert any("event: step" in chunk and '"step": "error"' in chunk for chunk in chunks)
    assert any("event: result" in chunk and '"status": "failed"' in chunk for chunk in chunks)
