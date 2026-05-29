"""Shared FastAPI dependencies.

Re-exports the most common dependencies so other routers can do:

    from api.deps import current_user, require_role, SessionDep, settings_dep
"""

from __future__ import annotations

from fastapi import Depends

from api.settings import Settings, get_settings
from auth.deps import current_user, require_role  # noqa: F401  (re-export)
from db.base import get_db  # noqa: F401  (re-export)


def settings_dep() -> Settings:
    return get_settings()


SettingsDep = Depends(settings_dep)
