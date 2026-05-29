"""Schemas for /settings/me and /settings/system."""

from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import BaseModel, Field


class UserPreferencesPayload(BaseModel):
    """Body of PUT /settings/me. All fields optional - partial updates allowed."""

    theme: Optional[Literal["light", "dark", "system"]] = None
    default_chart_type: Optional[Literal["auto", "bar", "line", "pie", "table"]] = None
    default_model: Optional[str] = Field(default=None, max_length=64)
    preferred_language: Optional[Literal["vi", "en"]] = None
    export_delimiter: Optional[str] = Field(default=None, max_length=4)


class UserPreferencesResponse(BaseModel):
    theme: Literal["light", "dark", "system"]
    default_chart_type: Literal["auto", "bar", "line", "pie", "table"]
    default_model: Optional[str]
    preferred_language: Literal["vi", "en"]
    export_delimiter: str


class SystemStatusResponse(BaseModel):
    trino: Literal["configured", "missing"]
    airflow: Literal["configured", "missing"]
    minio: Literal["configured", "missing"]
    groq: Literal["configured", "missing"]
    allow_temperature_override: bool
    model_whitelist: List[str]
