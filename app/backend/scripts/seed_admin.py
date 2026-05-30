"""Seed the bootstrap admin user.

Idempotent: safe to run on every container start.

- Reads APP_BOOTSTRAP_ADMIN_EMAIL and APP_BOOTSTRAP_ADMIN_PASSWORD from env.
- If the user does not exist, creates it with role='admin'.
- If the user exists, leaves its password untouched (so a deployed admin
  can rotate their password via the UI without it being clobbered).
- Always ensures the user has a user_preferences row.
"""

from __future__ import annotations

import os
import sys

import structlog

# Make `api`, `auth`, `db` imports work when the script is run as a module.
BACKEND_ROOT = "/opt/project/app/backend"
if BACKEND_ROOT not in sys.path:
    sys.path.insert(0, BACKEND_ROOT)

from api.logging import configure_logging
from auth import service as auth_service
from db.base import get_sessionmaker
from db.models import User


def main() -> int:
    configure_logging(os.environ.get("APP_ENV", "local"))
    log = structlog.get_logger("seed_admin")

    email = (os.environ.get("APP_BOOTSTRAP_ADMIN_EMAIL") or "").strip()
    password = os.environ.get("APP_BOOTSTRAP_ADMIN_PASSWORD") or ""

    if not email or not password:
        log.warning(
            "seed_admin.skipped",
            reason="APP_BOOTSTRAP_ADMIN_EMAIL or APP_BOOTSTRAP_ADMIN_PASSWORD missing",
        )
        return 0

    SessionLocal = get_sessionmaker()
    session = SessionLocal()
    try:
        existing = auth_service.get_user_by_email(session, email)
        if existing is None:
            user = User(
                email=email,
                password_hash=auth_service.hash_password(password),
                role="admin",
            )
            session.add(user)
            session.flush()
            auth_service.ensure_preferences(session, user)
            session.commit()
            log.info("seed_admin.created", email=email)
        else:
            # Leave password as-is; just ensure preferences row exists and role is admin.
            if existing.role != "admin":
                existing.role = "admin"
                session.add(existing)
            auth_service.ensure_preferences(session, existing)
            session.commit()
            log.info("seed_admin.exists", email=email)
        return 0
    except Exception as exc:
        session.rollback()
        log.error("seed_admin.failed", error=str(exc), error_type=exc.__class__.__name__)
        return 1
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
