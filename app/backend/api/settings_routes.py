from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from .auth import current_user, ensure_preferences
from .db import db_conn
from .settings import get_settings

router = APIRouter(prefix="/settings", tags=["settings"])

ALLOWED_THEMES = {"light", "dark", "system"}
ALLOWED_CHARTS = {"auto", "bar", "line", "pie", "table"}
ALLOWED_LANGS = {"vi", "en"}
ALLOWED_DELIMITERS = {",", ";", "\t"}


class PreferencesPatch(BaseModel):
    theme: str | None = None
    default_chart_type: str | None = None
    default_model: str | None = None
    preferred_language: str | None = None
    export_delimiter: str | None = None


@router.get("/me")
def get_me_settings(user: dict = Depends(current_user)) -> dict:
    return user["preferences"]


@router.put("/me")
def update_me_settings(body: PreferencesPatch, user: dict = Depends(current_user)) -> dict:
    patch: Dict[str, Any] = body.model_dump(exclude_unset=True)
    if "theme" in patch and patch["theme"] not in ALLOWED_THEMES:
        raise HTTPException(status_code=422, detail="Unsupported theme.")
    if "default_chart_type" in patch and patch["default_chart_type"] not in ALLOWED_CHARTS:
        raise HTTPException(status_code=422, detail="Unsupported chart type.")
    if "preferred_language" in patch and patch["preferred_language"] not in ALLOWED_LANGS:
        raise HTTPException(status_code=422, detail="Unsupported language.")
    if "export_delimiter" in patch and patch["export_delimiter"] not in ALLOWED_DELIMITERS:
        raise HTTPException(status_code=422, detail="Unsupported delimiter.")
    if not patch:
        return user["preferences"]

    fields = []
    values = []
    for key, value in patch.items():
        fields.append(f"{key} = %s")
        values.append(value)
    values.append(user["id"])
    with db_conn() as conn:
        ensure_preferences(conn, user["id"])
        conn.execute(
            f"""
            UPDATE app.user_preferences
            SET {", ".join(fields)}, updated_at = now()
            WHERE user_id = %s
            """,
            values,
        )
        return ensure_preferences(conn, user["id"])


@router.get("/system")
def get_system_settings(_user: dict = Depends(current_user)) -> dict:
    return get_settings().system_status

