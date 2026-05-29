"""Auth primitives: password hashing, JWT issue/verify, refresh-token rotation.

JWT design (matches docs/WEB_APP_PLAN.md Section 3.3 + Phase 2):
- Access token: short-lived, HS256, returned in the response body. Claims:
  {sub: user_id, email, role, type: "access", iat, exp, jti}
- Refresh token: long-lived, HS256, returned as HttpOnly cookie. Server keeps
  a row in app.refresh_tokens with `jti` so logout actually revokes.
"""

from __future__ import annotations

import secrets
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

import jwt
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.settings import Settings, get_settings
from db.models import RefreshToken, User, UserPreferences

_HASHER = PasswordHasher()


# ---------------------------------------------------------------------------
# Passwords
# ---------------------------------------------------------------------------


def hash_password(plain: str) -> str:
    return _HASHER.hash(plain)


def verify_password(plain: str, password_hash: str) -> bool:
    try:
        return _HASHER.verify(password_hash, plain)
    except VerifyMismatchError:
        return False
    except Exception:
        # Treat any other argon2 error as a non-match rather than a 500.
        return False


# ---------------------------------------------------------------------------
# JWTs
# ---------------------------------------------------------------------------


def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


def issue_access_token(user: User, settings: Optional[Settings] = None) -> Tuple[str, int]:
    settings = settings or get_settings()
    ttl = timedelta(minutes=settings.access_token_ttl_min)
    iat = _now_utc()
    payload = {
        "sub": str(user.id),
        "email": user.email,
        "role": user.role,
        "type": "access",
        "iat": int(iat.timestamp()),
        "exp": int((iat + ttl).timestamp()),
        "jti": secrets.token_urlsafe(16),
    }
    token = jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_alg)
    return token, int(ttl.total_seconds())


def issue_refresh_token(
    user: User,
    session: Session,
    settings: Optional[Settings] = None,
) -> Tuple[str, datetime, str]:
    """Returns (jwt, expires_at, jti). Persists the jti."""
    settings = settings or get_settings()
    ttl = timedelta(days=settings.refresh_token_ttl_days)
    iat = _now_utc()
    expires_at = iat + ttl
    jti = secrets.token_urlsafe(24)

    payload = {
        "sub": str(user.id),
        "type": "refresh",
        "iat": int(iat.timestamp()),
        "exp": int(expires_at.timestamp()),
        "jti": jti,
    }
    token = jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_alg)

    session.add(
        RefreshToken(
            user_id=user.id,
            jti=jti,
            expires_at=expires_at,
        )
    )
    session.flush()
    return token, expires_at, jti


def decode_token(token: str, settings: Optional[Settings] = None) -> dict:
    settings = settings or get_settings()
    return jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_alg])


# ---------------------------------------------------------------------------
# Refresh rotation + revoke
# ---------------------------------------------------------------------------


def rotate_refresh_token(
    session: Session,
    old_jti: str,
    user: User,
) -> Tuple[str, datetime]:
    revoke_refresh_token(session, old_jti)
    new_token, new_expires_at, _new_jti = issue_refresh_token(user, session)
    return new_token, new_expires_at


def revoke_refresh_token(session: Session, jti: str) -> None:
    row = session.execute(
        select(RefreshToken).where(RefreshToken.jti == jti)
    ).scalar_one_or_none()
    if row is None or row.revoked_at is not None:
        return
    row.revoked_at = _now_utc()
    session.add(row)
    session.flush()


def refresh_token_active(session: Session, jti: str) -> bool:
    row = session.execute(
        select(RefreshToken).where(RefreshToken.jti == jti)
    ).scalar_one_or_none()
    if row is None or row.revoked_at is not None:
        return False
    if row.expires_at <= _now_utc():
        return False
    return True


# ---------------------------------------------------------------------------
# User lookups
# ---------------------------------------------------------------------------


def get_user_by_email(session: Session, email: str) -> Optional[User]:
    return session.execute(
        select(User).where(User.email == email)
    ).scalar_one_or_none()


def get_user_by_id(session: Session, user_id: uuid.UUID) -> Optional[User]:
    return session.get(User, user_id)


def ensure_preferences(session: Session, user: User) -> UserPreferences:
    if user.preferences is not None:
        return user.preferences
    prefs = UserPreferences(user_id=user.id)
    session.add(prefs)
    session.flush()
    session.refresh(user)
    return user.preferences or prefs
