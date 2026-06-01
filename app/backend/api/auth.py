from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from uuid import UUID, uuid4

import jwt
from fastapi import APIRouter, Cookie, Depends, Header, HTTPException, Response, status
from pydantic import BaseModel, Field

from .db import db_conn, json_ready
from .settings import Settings, get_settings

router = APIRouter(prefix="/auth", tags=["auth"])

REFRESH_COOKIE = "agent4da_refresh"
REFRESH_PATH = "/auth"
HASH_ITERATIONS = 210_000


class LoginRequest(BaseModel):
    email: str = Field(min_length=3, max_length=320)
    password: str = Field(min_length=1, max_length=200)


class RegisterRequest(BaseModel):
    email: str = Field(min_length=3, max_length=320)
    password: str = Field(min_length=8, max_length=200)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, HASH_ITERATIONS)
    return "pbkdf2_sha256${}${}${}".format(
        HASH_ITERATIONS,
        base64.b64encode(salt).decode("ascii"),
        base64.b64encode(digest).decode("ascii"),
    )


def verify_password(password: str, encoded: str) -> bool:
    try:
        alg, iter_text, salt_text, digest_text = encoded.split("$", 3)
        if alg != "pbkdf2_sha256":
            return False
        salt = base64.b64decode(salt_text)
        expected = base64.b64decode(digest_text)
        actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, int(iter_text))
        return hmac.compare_digest(actual, expected)
    except Exception:
        return False


def _preferences(row: Optional[dict]) -> dict:
    if not row:
        return {
            "theme": "system",
            "default_chart_type": "auto",
            "default_model": None,
            "preferred_language": "vi",
            "export_delimiter": ",",
        }
    return {
        "theme": row.get("theme") or "system",
        "default_chart_type": row.get("default_chart_type") or "auto",
        "default_model": row.get("default_model"),
        "preferred_language": row.get("preferred_language") or "vi",
        "export_delimiter": row.get("export_delimiter") or ",",
    }


def ensure_preferences(conn, user_id: UUID) -> dict:
    conn.execute(
        """
        INSERT INTO app.user_preferences (user_id)
        VALUES (%s)
        ON CONFLICT (user_id) DO NOTHING
        """,
        (user_id,),
    )
    row = conn.execute(
        "SELECT * FROM app.user_preferences WHERE user_id = %s",
        (user_id,),
    ).fetchone()
    return _preferences(row)


def user_payload(user: dict, prefs: Optional[dict] = None) -> dict:
    return {
        "id": str(user["id"]),
        "email": user["email"],
        "role": user["role"],
        "created_at": json_ready(user["created_at"]),
        "preferences": prefs,
    }


def issue_access_token(user: dict, settings: Settings) -> tuple[str, int]:
    ttl = timedelta(minutes=settings.access_token_ttl_min)
    issued_at = _now()
    payload = {
        "sub": str(user["id"]),
        "email": user["email"],
        "role": user["role"],
        "type": "access",
        "iat": int(issued_at.timestamp()),
        "exp": int((issued_at + ttl).timestamp()),
        "jti": secrets.token_urlsafe(16),
    }
    return jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_alg), int(ttl.total_seconds())


def issue_refresh_token(conn, user: dict, settings: Settings) -> tuple[str, datetime]:
    expires_at = _now() + timedelta(days=settings.refresh_token_ttl_days)
    jti = secrets.token_urlsafe(24)
    payload = {
        "sub": str(user["id"]),
        "type": "refresh",
        "iat": int(_now().timestamp()),
        "exp": int(expires_at.timestamp()),
        "jti": jti,
    }
    token = jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_alg)
    conn.execute(
        "INSERT INTO app.refresh_tokens (jti, user_id, expires_at) VALUES (%s, %s, %s)",
        (jti, user["id"], expires_at),
    )
    return token, expires_at


def _set_refresh_cookie(response: Response, token: str, expires_at: datetime, settings: Settings) -> None:
    response.set_cookie(
        key=REFRESH_COOKIE,
        value=token,
        max_age=max(0, int((expires_at - _now()).total_seconds())),
        expires=expires_at,
        path=REFRESH_PATH,
        httponly=True,
        secure=settings.app_env != "local",
        samesite="lax",
    )


def _clear_refresh_cookie(response: Response, settings: Settings) -> None:
    response.delete_cookie(
        key=REFRESH_COOKIE,
        path=REFRESH_PATH,
        httponly=True,
        secure=settings.app_env != "local",
        samesite="lax",
    )


def decode_token(token: str, settings: Optional[Settings] = None) -> dict:
    settings = settings or get_settings()
    return jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_alg])


def current_user(authorization: str = Header(default="")) -> dict:
    if not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token.")
    token = authorization.split(" ", 1)[1].strip()
    try:
        payload = decode_token(token)
    except jwt.PyJWTError as exc:
        raise HTTPException(status_code=401, detail="Invalid or expired token.") from exc
    if payload.get("type") != "access":
        raise HTTPException(status_code=401, detail="Wrong token type.")
    with db_conn() as conn:
        user = conn.execute("SELECT * FROM app.users WHERE id = %s", (payload["sub"],)).fetchone()
        if not user:
            raise HTTPException(status_code=401, detail="User no longer exists.")
        prefs = ensure_preferences(conn, user["id"])
        user = dict(user)
        user["preferences"] = prefs
        return user


def require_admin(user: dict = Depends(current_user)) -> dict:
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin role required.")
    return user


def seed_admin() -> None:
    settings = get_settings()
    if not settings.bootstrap_admin_email or not settings.bootstrap_admin_password:
        return
    try:
        with db_conn() as conn:
            user = conn.execute(
                "SELECT * FROM app.users WHERE lower(email) = lower(%s)",
                (settings.bootstrap_admin_email,),
            ).fetchone()
            if user:
                if settings.app_env == "local":
                    conn.execute(
                        "UPDATE app.users SET role = 'admin', password_hash = %s WHERE id = %s",
                        (hash_password(settings.bootstrap_admin_password), user["id"]),
                    )
                else:
                    conn.execute("UPDATE app.users SET role = 'admin' WHERE id = %s", (user["id"],))
                ensure_preferences(conn, user["id"])
                return
            user_id = uuid4()
            conn.execute(
                """
                INSERT INTO app.users (id, email, password_hash, role)
                VALUES (%s, %s, %s, 'admin')
                """,
                (user_id, settings.bootstrap_admin_email, hash_password(settings.bootstrap_admin_password)),
            )
            ensure_preferences(conn, user_id)
    except Exception:
        return


def token_response(conn, response: Response, user: dict, settings: Settings) -> dict:
    prefs = ensure_preferences(conn, user["id"])
    access, expires_in = issue_access_token(user, settings)
    refresh, refresh_expires_at = issue_refresh_token(conn, user, settings)
    _set_refresh_cookie(response, refresh, refresh_expires_at, settings)
    return {
        "access_token": access,
        "token_type": "bearer",
        "expires_in": expires_in,
        "user": user_payload(user, prefs),
    }


@router.post("/register", status_code=201)
def register(body: RegisterRequest, response: Response, settings: Settings = Depends(get_settings)) -> dict:
    email = body.email.strip().lower()
    if "@" not in email or email.startswith("@") or email.endswith("@"):
        raise HTTPException(status_code=422, detail="Please enter a valid email address.")

    with db_conn() as conn:
        existing = conn.execute(
            "SELECT id FROM app.users WHERE lower(email) = lower(%s)",
            (email,),
        ).fetchone()
        if existing:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="This email is already registered.")

        user_id = uuid4()
        conn.execute(
            """
            INSERT INTO app.users (id, email, password_hash, role)
            VALUES (%s, %s, %s, 'user')
            """,
            (user_id, email, hash_password(body.password)),
        )
        user = conn.execute("SELECT * FROM app.users WHERE id = %s", (user_id,)).fetchone()
        return token_response(conn, response, user, settings)


@router.post("/login")
def login(body: LoginRequest, response: Response, settings: Settings = Depends(get_settings)) -> dict:
    with db_conn() as conn:
        user = conn.execute(
            "SELECT * FROM app.users WHERE lower(email) = lower(%s)",
            (body.email,),
        ).fetchone()
        if not user or not verify_password(body.password, user["password_hash"]):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid email or password.")
        return token_response(conn, response, user, settings)


@router.post("/refresh")
def refresh(
    response: Response,
    refresh_cookie: Optional[str] = Cookie(default=None, alias=REFRESH_COOKIE),
    settings: Settings = Depends(get_settings),
) -> dict:
    if not refresh_cookie:
        raise HTTPException(status_code=401, detail="Missing refresh token.")
    try:
        payload = decode_token(refresh_cookie, settings)
    except jwt.PyJWTError as exc:
        raise HTTPException(status_code=401, detail="Invalid refresh token.") from exc
    if payload.get("type") != "refresh" or not payload.get("jti"):
        raise HTTPException(status_code=401, detail="Wrong token type.")
    with db_conn() as conn:
        active = conn.execute(
            """
            SELECT * FROM app.refresh_tokens
            WHERE jti = %s AND revoked_at IS NULL AND expires_at > now()
            """,
            (payload["jti"],),
        ).fetchone()
        if not active:
            raise HTTPException(status_code=401, detail="Refresh token revoked or expired.")
        user = conn.execute("SELECT * FROM app.users WHERE id = %s", (payload["sub"],)).fetchone()
        if not user:
            raise HTTPException(status_code=401, detail="User no longer exists.")
        conn.execute("UPDATE app.refresh_tokens SET revoked_at = now() WHERE jti = %s", (payload["jti"],))
        return token_response(conn, response, user, settings)


@router.post("/logout", status_code=204)
def logout(
    response: Response,
    refresh_cookie: Optional[str] = Cookie(default=None, alias=REFRESH_COOKIE),
    settings: Settings = Depends(get_settings),
) -> Response:
    if refresh_cookie:
        try:
            payload = decode_token(refresh_cookie, settings)
            if payload.get("jti"):
                with db_conn() as conn:
                    conn.execute("UPDATE app.refresh_tokens SET revoked_at = now() WHERE jti = %s", (payload["jti"],))
        except Exception:
            pass
    _clear_refresh_cookie(response, settings)
    return Response(status_code=204)


@router.get("/me")
def me(user: dict = Depends(current_user)) -> dict:
    return user_payload(user, user.get("preferences"))
