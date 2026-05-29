"""FastAPI dependencies for resolving the current authenticated user.

Other routers (history, agent, settings, pipelines, ...) import:
    from auth.deps import current_user, require_role

`require_role("admin")` is the guard for the DAG-trigger endpoint.
"""

from __future__ import annotations

from typing import Callable

import jwt
import structlog
from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from api.settings import Settings, get_settings
from auth import service as auth_service
from db.base import get_db
from db.models import User

log = structlog.get_logger("auth.deps")


def _extract_bearer(request: Request) -> str:
    auth_header = request.headers.get("authorization") or request.headers.get("Authorization")
    if not auth_header or not auth_header.lower().startswith("bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing bearer token.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return auth_header.split(" ", 1)[1].strip()


def current_user(
    request: Request,
    session: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> User:
    token = _extract_bearer(request)
    try:
        payload = auth_service.decode_token(token, settings)
    except jwt.PyJWTError as exc:
        log.info("auth.decode.failed", error=str(exc))
        raise HTTPException(
            status_code=401,
            detail="Invalid or expired token.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if payload.get("type") != "access":
        raise HTTPException(status_code=401, detail="Wrong token type.")

    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="Malformed token.")

    user = auth_service.get_user_by_id(session, user_id)
    if user is None:
        raise HTTPException(status_code=401, detail="User no longer exists.")
    return user


def require_role(role: str) -> Callable[..., User]:
    """Dependency factory: require_role('admin')."""

    def _dep(user: User = Depends(current_user)) -> User:
        if user.role != role and not (role == "user" and user.role == "admin"):
            # An admin can do anything a "user" can; reverse is not true.
            raise HTTPException(status_code=403, detail=f"Requires role '{role}'.")
        return user

    return _dep
