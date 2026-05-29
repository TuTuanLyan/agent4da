"""Pydantic request/response schemas for the auth router."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional
from uuid import UUID

from pydantic import BaseModel, EmailStr, Field


class LoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=1, max_length=200)


class TokenResponse(BaseModel):
    access_token: str
    token_type: Literal["bearer"] = "bearer"
    expires_in: int  # seconds until access token expiry
    user: "MeResponse"


class MeResponse(BaseModel):
    id: UUID
    email: EmailStr
    role: Literal["user", "admin"]
    created_at: datetime
    preferences: Optional["UserPreferencesOut"] = None


class UserPreferencesOut(BaseModel):
    theme: Literal["light", "dark", "system"]
    default_chart_type: Literal["auto", "bar", "line", "pie", "table"]
    default_model: Optional[str]
    preferred_language: Literal["vi", "en"]
    export_delimiter: str


TokenResponse.model_rebuild()
MeResponse.model_rebuild()
