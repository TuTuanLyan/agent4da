"""Pydantic schemas for the catalog router."""

from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import BaseModel


TableKind = Literal["fact", "dimension", "summary", "semantic"]


class CatalogColumn(BaseModel):
    table_name: str
    column_name: str
    data_type: Optional[str] = None
    meaning: Optional[str] = None
    business_terms: Optional[str] = None
    example_usage: Optional[str] = None


class CatalogTable(BaseModel):
    table_name: str
    display_name: Optional[str] = None
    purpose: Optional[str] = None
    grain: Optional[str] = None
    use_for: Optional[str] = None
    query_notes: Optional[str] = None
    kind: TableKind
    column_count: int = 0


class CatalogTableDetail(CatalogTable):
    columns: List[CatalogColumn]


class CatalogSearchHit(BaseModel):
    kind: Literal["table", "column"]
    table_name: str
    column_name: Optional[str] = None
    display_name: Optional[str] = None
    snippet: str


class CatalogSearchResponse(BaseModel):
    query: str
    hits: List[CatalogSearchHit]
