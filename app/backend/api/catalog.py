from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from .auth import current_user
from .integrations import load_catalog_metadata

router = APIRouter(prefix="/catalog", tags=["catalog"])


def table_kind(table_name: str) -> str:
    short = table_name.split(".")[-1].lower()
    if short.startswith("fact_"):
        return "fact"
    if short.startswith("dim_"):
        return "dimension"
    if "summary" in short:
        return "summary"
    if "semantic" in short:
        return "semantic"
    return "semantic"


def table_row(table: dict) -> dict:
    columns = table.get("columns") or []
    return {
        "table_name": table.get("table_name") or "",
        "display_name": table.get("display_name") or None,
        "purpose": table.get("purpose") or None,
        "grain": table.get("grain") or None,
        "use_for": table.get("use_for") or None,
        "query_notes": table.get("query_notes") or None,
        "kind": table_kind(table.get("table_name") or ""),
        "column_count": len(columns),
    }


def column_row(column: dict) -> dict:
    return {
        "column_name": column.get("column_name") or "",
        "data_type": column.get("data_type") or None,
        "meaning": column.get("meaning") or None,
        "business_terms": column.get("business_terms") or None,
        "example_usage": column.get("example_usage") or None,
    }


@router.get("/tables")
def list_tables(_user: dict = Depends(current_user)) -> list[dict]:
    metadata = load_catalog_metadata()
    return [table_row(table) for table in metadata.get("tables") or []]


@router.get("/tables/{table_name:path}")
def get_table(table_name: str, _user: dict = Depends(current_user)) -> dict:
    metadata = load_catalog_metadata()
    decoded = table_name.strip()
    for table in metadata.get("tables") or []:
        if table.get("table_name") == decoded:
            row = table_row(table)
            row["columns"] = [column_row(col) for col in table.get("columns") or []]
            return row
    raise HTTPException(status_code=404, detail="Unknown table.")


@router.get("/search")
def search_catalog(
    q: str = Query(min_length=1),
    limit: int = Query(default=20, ge=1, le=100),
    _user: dict = Depends(current_user),
) -> dict:
    query = q.strip().lower()
    metadata = load_catalog_metadata()
    hits: list[dict] = []
    for table in metadata.get("tables") or []:
        haystack = " ".join(
            str(table.get(key) or "")
            for key in ("table_name", "display_name", "purpose", "grain", "use_for", "query_notes")
        ).lower()
        if query in haystack:
            hits.append(
                {
                    "kind": "table",
                    "table_name": table.get("table_name"),
                    "column_name": None,
                    "display_name": table.get("display_name"),
                    "snippet": table.get("purpose") or table.get("use_for") or "",
                }
            )
        for column in table.get("columns") or []:
            col_haystack = " ".join(
                str(column.get(key) or "")
                for key in ("column_name", "data_type", "meaning", "business_terms", "example_usage")
            ).lower()
            if query in col_haystack:
                hits.append(
                    {
                        "kind": "column",
                        "table_name": table.get("table_name"),
                        "column_name": column.get("column_name"),
                        "display_name": table.get("display_name"),
                        "snippet": column.get("meaning") or column.get("business_terms") or "",
                    }
                )
        if len(hits) >= limit:
            break
    return {"query": q, "hits": hits[:limit]}

