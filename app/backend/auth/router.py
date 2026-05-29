"""Auth router.

Endpoints:
  POST /auth/login    - email+password -> access token + HttpOnly refresh cookie.
  POST /auth/refresh  - refresh cookie -> rotated access + refresh token.
  POST /auth/logout   - revoke current refresh token, clear cookie.
  GET  /auth/me       - return the current user.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

import jwt
import structlog
from fastapi import APIRouter, Cookie, Depends, HTTPException, Response, status
from sqlalchemy.orm import Session

from api.settings import Settings, get_settings
from auth import service as auth_service
from auth.deps import current_user
from auth.schemas import (
    LoginRequest,
    MeResponse,
    TokenResponse,
    UserPreferencesOut,
)
from db.base import get_db
from db.models import User

log = structlog.get_logger("auth.router")

router = APIRouter(prefix="/auth", tags=["auth"])

REFRESH_COOKIE_NAME = "agent4da_refresh"
REFRESH_COOKIE_PATH = "/auth"


def _me_response(user: User) -> MeResponse:
    prefs = user.preferences
    prefs_out: Optional[UserPreferencesOut] = None
    if prefs is not None:
        prefs_out = UserPreferencesOut(
            theme=prefs.theme,                              # type: ignore[arg-type]
            default_chart_type=prefs.default_chart_type,    # type: ignore[arg-type]
            default_model=prefs.default_model,
            preferred_language=prefs.preferred_language,    # type: ignore[arg-type]
            export_delimiter=prefs.export_delimiter,
        )
    return MeResponse(
        id=user.id,
        email=user.email,
        role=user.role,                                     # type: ignore[arg-type]
        created_at=user.created_at,
        preferences=prefs_out,
    )


def _set_refresh_cookie(
    response: Response,
    token: str,
    expires_at: datetime,
    settings: Settings,
) -> None:
    response.set_cookie(
        key=REFRESH_COOKIE_NAME,
        value=token,
        max_age=int((expires_at - datetime.now(expires_at.tzinfo)).total_seconds()),
        expires=expires_at,
        path=REFRESH_COOKIE_PATH,
        httponly=True,
        secure=settings.app_env != "local",
        samesite="lax",
    )


def _clear_refresh_cookie(response: Response, settings: Settings) -> None:
    response.delete_cookie(
        key=REFRESH_COOKIE_NAME,
        path=REFRESH_COOKIE_PATH,
        httponly=True,
        secure=settings.app_env != "local",
        samesite="lax",
    )


@router.post("/login", response_model=TokenResponse)
def login(
    body: LoginRequest,
    response: Response,
    session: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> TokenResponse:
    user = auth_service.get_user_by_email(session, body.email)
    if user is None or not auth_service.verify_password(body.password, user.password_hash):
        # Same error message for both cases so we don't leak which.
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password.",
        )

    # Ensure the user has a preferences row.
    auth_service.ensure_preferences(session, user)

    access_token, expires_in = auth_service.issue_access_token(user, settings)
    refresh_token, refresh_expires_at, _ = auth_service.issue_refresh_token(
        user, session, settings
    )
    session.commit()
    session.refresh(user)

    _set_refresh_cookie(response, refresh_token, refresh_expires_at, settings)

    log.info("auth.login.ok", user_id=str(user.id), email=user.email)

    return TokenResponse(
        access_token=access_token,
        token_type="bearer",
        expires_in=expires_in,
        user=_me_response(user),
    )


@router.post("/refresh", response_model=TokenResponse)
def refresh(
    response: Response,
    refresh_cookie: Optional[str] = Cookie(default=None, alias=REFRESH_COOKIE_NAME),
    session: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> TokenResponse:
    if not refresh_cookie:
        raise HTTPException(status_code=401, detail="Missing refresh token.")

    try:
        payload = auth_service.decode_token(refresh_cookie, settings)
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid refresh token.")

    if payload.get("type") != "refresh":
        raise HTTPException(status_code=401, detail="Wrong token type.")

    jti = payload.get("jti")
    if not jti or not auth_service.refresh_token_active(session, jti):
        raise HTTPException(status_code=401, detail="Refresh token revoked or expired.")

    user = auth_service.get_user_by_id(session, payload["sub"])
    if user is None:
        raise HTTPException(status_code=401, detail="User no longer exists.")

    # Rotate refresh token.
    new_refresh, new_refresh_expires_at = auth_service.rotate_refresh_token(
        session, jti, user
    )
    access_token, expires_in = auth_service.issue_access_token(user, settings)
    session.commit()
    session.refresh(user)

    _set_refresh_cookie(response, new_refresh, new_refresh_expires_at, settings)

    return TokenResponse(
        access_token=access_token,
        token_type="bearer",
        expires_in=expires_in,
        user=_me_response(user),
    )


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
def logout(
    response: Response,
    refresh_cookie: Optional[str] = Cookie(default=None, alias=REFRESH_COOKIE_NAME),
    session: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> Response:
    if refresh_cookie:
        try:
            payload = auth_service.decode_token(refresh_cookie, settings)
            if payload.get("type") == "refresh" and payload.get("jti"):
                auth_service.revoke_refresh_token(session, payload["jti"])
                session.commit()
        except jwt.PyJWTError:
            pass
    _clear_refresh_cookie(response, settings)
    return Response(status_code=204)


@router.get("/me", response_model=MeResponse)
def me(
    session: Session = Depends(get_db),
    user: User = Depends(current_user),
) -> MeResponse:
    # Make sure preferences are loaded; the relationship is lazy.
    if user.preferences is None:
        auth_service.ensure_preferences(session, user)
        session.commit()
        session.refresh(user)
    return _me_response(user)
