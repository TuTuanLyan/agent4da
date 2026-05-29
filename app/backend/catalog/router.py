"""/catalog router (read-only).

  GET /catalog/tables                 - list of semantic tables with kind + column_count.
  GET /catalog/tables/{table_name}    - table row + its columns.
  GET /catalog/columns?table_name=&q= - filtered columns.
  GET /catalog/search?q=              - top hits across tables + columns.

Auth required; the catalog is the agent's view of Gold, not public data.
"""

from __future__ import annotations

from typing import List, Optional

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query

from api.errors import public_error_message
from auth.deps import current_user
from catalog import service as catalog_service
from catalog.schemas import (
    CatalogColumn,
    CatalogSearchResponse,
    CatalogTable,
    CatalogTableDetail,
)
from db.models import User


log = structlog.get_logger("catalog.router")
router = APIRouter(prefix="/catalog", tags=["catalog"])


def _trino_error_to_503(exc: Exception) -> HTTPException:
    log.warning("catalog.trino_unavailable", error=f"{exc.__class__.__name__}: {exc}")
    return HTTPException(
        status_code=503,
        detail=public_error_message(
            exc,
            "Catalog source unavailable. Check Trino and retry.",
        ),
    )


@router.get("/tables", response_model=List[CatalogTable])
def list_tables(user: User = Depends(current_user)) -> List[CatalogTable]:
    try:
        return catalog_service.list_tables()
    except Exception as exc:
        raise _trino_error_to_503(exc)


@router.get("/tables/{table_name}", response_model=CatalogTableDetail)
def get_table_detail(
    table_name: str,
    user: User = Depends(current_user),
) -> CatalogTableDetail:
    try:
        detail = catalog_service.get_table_detail(table_name)
    except Exception as exc:
        raise _trino_error_to_503(exc)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"Unknown table: {table_name}")
    return detail


@router.get("/columns", response_model=List[CatalogColumn])
def list_columns(
    table_name: Optional[str] = Query(default=None, max_length=128),
    q: Optional[str] = Query(default=None, max_length=128),
    user: User = Depends(current_user),
) -> List[CatalogColumn]:
    try:
        return catalog_service.list_columns(table_name=table_name, q=q)
    except Exception as exc:
        raise _trino_error_to_503(exc)


@router.get("/search", response_model=CatalogSearchResponse)
def search(
    q: str = Query(..., min_length=1, max_length=200),
    limit: int = Query(default=20, ge=1, le=50),
    user: User = Depends(current_user),
) -> CatalogSearchResponse:
    try:
        return catalog_service.search(q, limit=limit)
    except Exception as exc:
        raise _trino_error_to_503(exc)
