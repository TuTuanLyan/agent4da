"""/settings router.

GET  /settings/me     - current user preferences (auth required).
PUT  /settings/me     - update preferences (partial).
GET  /settings/system - redacted view of which integrations are configured.
"""

from __future__ import annotations

import structlog
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from api.settings import Settings, get_settings
from auth import service as auth_service
from auth.deps import current_user
from db.base import get_db
from db.models import User
from settings_router.schemas import (
    SystemStatusResponse,
    UserPreferencesPayload,
    UserPreferencesResponse,
)

log = structlog.get_logger("settings.router")

router = APIRouter(prefix="/settings", tags=["settings"])


def _prefs_response(prefs) -> UserPreferencesResponse:
    return UserPreferencesResponse(
        theme=prefs.theme,
        default_chart_type=prefs.default_chart_type,
        default_model=prefs.default_model,
        preferred_language=prefs.preferred_language,
        export_delimiter=prefs.export_delimiter,
    )


@router.get("/me", response_model=UserPreferencesResponse)
def get_me(
    session: Session = Depends(get_db),
    user: User = Depends(current_user),
) -> UserPreferencesResponse:
    prefs = auth_service.ensure_preferences(session, user)
    session.commit()
    return _prefs_response(prefs)


@router.put("/me", response_model=UserPreferencesResponse)
def update_me(
    body: UserPreferencesPayload,
    session: Session = Depends(get_db),
    user: User = Depends(current_user),
    settings: Settings = Depends(get_settings),
) -> UserPreferencesResponse:
    prefs = auth_service.ensure_preferences(session, user)

    if body.default_model is not None:
        if body.default_model not in settings.model_whitelist_list:
            raise HTTPException(
                status_code=422,
                detail=(
                    "default_model must be one of: "
                    + ", ".join(settings.model_whitelist_list)
                ),
            )
        prefs.default_model = body.default_model

    if body.theme is not None:
        prefs.theme = body.theme
    if body.default_chart_type is not None:
        prefs.default_chart_type = body.default_chart_type
    if body.preferred_language is not None:
        prefs.preferred_language = body.preferred_language
    if body.export_delimiter is not None:
        prefs.export_delimiter = body.export_delimiter

    session.add(prefs)
    session.commit()
    session.refresh(prefs)

    log.info("settings.me.update", user_id=str(user.id))
    return _prefs_response(prefs)


@router.get("/system", response_model=SystemStatusResponse)
def get_system(
    user: User = Depends(current_user),
    settings: Settings = Depends(get_settings),
) -> SystemStatusResponse:
    # Auth required so anonymous probes don't enumerate which integrations
    # are configured. The response never contains secret values.
    status = settings.system_status
    return SystemStatusResponse(**status)
