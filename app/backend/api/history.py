from __future__ import annotations

from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Response

from .agent import format_run
from .auth import current_user
from .db import db_conn

router = APIRouter(prefix="/history", tags=["history"])

VALID_STATUSES = {"running", "success", "failed", "stopped", "blocked"}


def _uuid(value: str) -> UUID:
    try:
        return UUID(value)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Run not found.") from exc


@router.get("")
def list_history(
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=25, ge=1, le=100),
    status: Optional[str] = None,
    favorite: bool = False,
    q: Optional[str] = None,
    from_: Optional[str] = Query(default=None, alias="from"),
    to: Optional[str] = None,
    user: dict = Depends(current_user),
) -> dict:
    clauses = ["r.user_id = %s"]
    values: list = [user["id"]]
    if status:
        statuses = [item for item in status.split(",") if item in VALID_STATUSES]
        if statuses:
            clauses.append("r.status = ANY(%s)")
            values.append(statuses)
    if favorite:
        clauses.append("f.run_id IS NOT NULL")
    if q:
        clauses.append("r.question ILIKE %s")
        values.append(f"%{q}%")
    if from_:
        clauses.append("r.created_at >= %s")
        values.append(from_)
    if to:
        clauses.append("r.created_at <= %s")
        values.append(to)
    where = " AND ".join(clauses)
    offset = (page - 1) * limit
    with db_conn() as conn:
        total = conn.execute(
            f"""
            SELECT count(*) AS n
            FROM app.query_runs r
            LEFT JOIN app.favorite_runs f ON f.run_id = r.id AND f.user_id = r.user_id
            WHERE {where}
            """,
            values,
        ).fetchone()["n"]
        rows = conn.execute(
            f"""
            SELECT r.*, (f.run_id IS NOT NULL) AS is_favorite
            FROM app.query_runs r
            LEFT JOIN app.favorite_runs f ON f.run_id = r.id AND f.user_id = r.user_id
            WHERE {where}
            ORDER BY r.created_at DESC
            LIMIT %s OFFSET %s
            """,
            [*values, limit, offset],
        ).fetchall()
    items = [
        {
            "run_id": str(row["id"]),
            "question": row["question"],
            "status": row["status"],
            "guard_status": row.get("guard_status"),
            "row_count": row.get("row_count") or 0,
            "latency_ms": row.get("latency_ms"),
            "is_favorite": bool(row.get("is_favorite")),
            "has_summary": bool(row.get("summary")),
            "created_at": row["created_at"].isoformat(),
        }
        for row in rows
    ]
    return {"items": items, "total": int(total), "page": page, "limit": limit, "has_next": offset + len(items) < total}


@router.get("/{run_id}")
def get_history_run(run_id: str, user: dict = Depends(current_user)) -> dict:
    with db_conn() as conn:
        row = conn.execute(
            """
            SELECT r.*, (f.run_id IS NOT NULL) AS is_favorite
            FROM app.query_runs r
            LEFT JOIN app.favorite_runs f ON f.run_id = r.id AND f.user_id = r.user_id
            WHERE r.id = %s AND r.user_id = %s
            """,
            (_uuid(run_id), user["id"]),
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Run not found.")
    return format_run(row, row.get("is_favorite"))


@router.post("/{run_id}/favorite", status_code=204)
def favorite_run(run_id: str, user: dict = Depends(current_user)) -> Response:
    with db_conn() as conn:
        exists = conn.execute(
            "SELECT 1 FROM app.query_runs WHERE id = %s AND user_id = %s",
            (_uuid(run_id), user["id"]),
        ).fetchone()
        if not exists:
            raise HTTPException(status_code=404, detail="Run not found.")
        conn.execute(
            """
            INSERT INTO app.favorite_runs (user_id, run_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
            """,
            (user["id"], _uuid(run_id)),
        )
    return Response(status_code=204)


@router.delete("/{run_id}/favorite", status_code=204)
def unfavorite_run(run_id: str, user: dict = Depends(current_user)) -> Response:
    with db_conn() as conn:
        conn.execute(
            "DELETE FROM app.favorite_runs WHERE user_id = %s AND run_id = %s",
            (user["id"], _uuid(run_id)),
        )
    return Response(status_code=204)

