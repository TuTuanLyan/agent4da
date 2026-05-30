"""ORM models for the `app` schema.

Phase 2 ships three tables. Later phases extend this module with
query_runs, sample_questions, layer_stats, etc.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, List, Optional

from sqlalchemy import (
    Boolean,
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Float,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import CITEXT, JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base import Base


def _uuid_pk() -> Mapped[uuid.UUID]:
    return mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=func.gen_random_uuid(),
    )


class User(Base):
    __tablename__ = "users"
    __table_args__ = (
        CheckConstraint("role in ('user','admin')", name="users_role_check"),
    )

    id: Mapped[uuid.UUID] = _uuid_pk()
    email: Mapped[str] = mapped_column(CITEXT, unique=True, nullable=False, index=True)
    password_hash: Mapped[str] = mapped_column(Text, nullable=False)
    role: Mapped[str] = mapped_column(
        String(16), nullable=False, default="user", server_default="user"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    preferences: Mapped[Optional["UserPreferences"]] = relationship(
        back_populates="user", uselist=False, cascade="all, delete-orphan"
    )
    refresh_tokens: Mapped[list["RefreshToken"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )


class RefreshToken(Base):
    __tablename__ = "refresh_tokens"

    id: Mapped[uuid.UUID] = _uuid_pk()
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("app.users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    jti: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    revoked_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    user: Mapped[User] = relationship(back_populates="refresh_tokens")


class UserPreferences(Base):
    __tablename__ = "user_preferences"
    __table_args__ = (
        CheckConstraint(
            "theme in ('light','dark','system')", name="user_preferences_theme_check"
        ),
        CheckConstraint(
            "default_chart_type in ('auto','bar','line','pie','table')",
            name="user_preferences_chart_check",
        ),
        CheckConstraint(
            "preferred_language in ('vi','en')",
            name="user_preferences_lang_check",
        ),
    )

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("app.users.id", ondelete="CASCADE"),
        primary_key=True,
    )
    theme: Mapped[str] = mapped_column(
        String(16), nullable=False, default="system", server_default="system"
    )
    default_chart_type: Mapped[str] = mapped_column(
        String(16), nullable=False, default="auto", server_default="auto"
    )
    default_model: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    preferred_language: Mapped[str] = mapped_column(
        String(8), nullable=False, default="vi", server_default="vi"
    )
    export_delimiter: Mapped[str] = mapped_column(
        String(4), nullable=False, default=",", server_default=","
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    user: Mapped[User] = relationship(back_populates="preferences")


class ChatSession(Base):
    """A conversation thread on the Ask page.

    Successive Ask runs that share a session let the agent use earlier turns as
    follow-up context. Deleting a session keeps its `query_runs` rows (the FK on
    `query_runs.session_id` is ON DELETE SET NULL), so history is preserved.
    """

    __tablename__ = "chat_sessions"

    id: Mapped[uuid.UUID] = _uuid_pk()
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("app.users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    title: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    is_pinned: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="false", index=True
    )
    pinned_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    last_used_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False, index=True
    )


class QueryRun(Base):
    __tablename__ = "query_runs"
    __table_args__ = (
        CheckConstraint(
            "status in ('running','success','failed','stopped','blocked')",
            name="query_runs_status_check",
        ),
        CheckConstraint(
            "guard_status is null or guard_status in ('pass','blocked','auto_limited','error')",
            name="query_runs_guard_status_check",
        ),
        CheckConstraint(
            "chart_type is null or chart_type in ('auto','bar','line','pie','table','scatter')",
            name="query_runs_chart_type_check",
        ),
    )

    id: Mapped[uuid.UUID] = _uuid_pk()
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("app.users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    # A run may belong to a chat session. SET NULL on delete so removing a chat
    # keeps the run in History (just detached from the thread).
    session_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("app.chat_sessions.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    turn_index: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    question: Mapped[str] = mapped_column(Text, nullable=False)
    generated_sql: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    guard_status: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    row_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    latency_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    summary_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    insights: Mapped[Optional[List[Any]]] = mapped_column(JSONB, nullable=True)
    key_numbers: Mapped[Optional[List[Any]]] = mapped_column(JSONB, nullable=True)
    chart_type: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    chart_suggestion: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    columns: Mapped[Optional[List[Any]]] = mapped_column(JSONB, nullable=True)
    result_json: Mapped[Optional[List[Any]]] = mapped_column(JSONB, nullable=True)
    trino_query_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    # --- Agent v2 columns (migration 0007_agent_v2) ------------------------
    # Full chart recommendation dict + the small (<=20 row) chart series the
    # frontend renders; the agent trace holds NLU/metadata/retry/context info.
    chart_payload: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    chart_data: Mapped[Optional[List[Any]]] = mapped_column(JSONB, nullable=True)
    agent_trace: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    retry_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    model_used: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    agent_engine: Mapped[str] = mapped_column(
        String(16), nullable=False, default="legacy", server_default="legacy"
    )
    is_favorite: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="false"
    )
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, default="success", server_default="success"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class SampleQuestion(Base):
    __tablename__ = "sample_questions"

    id: Mapped[uuid.UUID] = _uuid_pk()
    label: Mapped[str] = mapped_column(String(64), nullable=False)
    question: Mapped[str] = mapped_column(Text, nullable=False)
    sort_order: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default="true"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class PipelineTriggerAudit(Base):
    __tablename__ = "pipeline_trigger_audit"
    __table_args__ = (
        CheckConstraint(
            "status in ('triggered','failed')",
            name="pipeline_audit_status_check",
        ),
    )

    id: Mapped[uuid.UUID] = _uuid_pk()
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("app.users.id", ondelete="CASCADE"),
        nullable=False,
    )
    dag_id: Mapped[str] = mapped_column(String(128), nullable=False)
    airflow_run_id: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    conf: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, default="triggered", server_default="triggered"
    )
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class LayerStat(Base):
    __tablename__ = "layer_stats"
    __table_args__ = (
        CheckConstraint(
            "layer in ('bronze','silver','gold','metadata')",
            name="layer_stats_layer_check",
        ),
    )

    id: Mapped[uuid.UUID] = _uuid_pk()
    layer: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    metric_name: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    metric_value_bigint: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    metric_value_double: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    metric_unit: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    source: Mapped[str] = mapped_column(String(64), nullable=False, server_default="scheduler")
    detail: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    measured_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False, index=True
    )


class AgentCheckpointSnapshot(Base):
    """Compact final-state snapshot written after each v2 agent run.

    Replaces the langgraph Postgres saver (not installed) with a durable record
    of the conversation thread state per turn. Keyed by thread_id (the chat
    session id) and a per-turn checkpoint_id.
    """

    __tablename__ = "agent_checkpoint_snapshots"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    thread_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    checkpoint_id: Mapped[str] = mapped_column(String(160), nullable=False)
    state_data: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class AgentFeedback(Base):
    """User feedback and suggestion clicks for contextual learning."""

    __tablename__ = "agent_feedback"
    __table_args__ = (
        CheckConstraint(
            "feedback_type in ('positive','negative','suggestion_click','free_text')",
            name="agent_feedback_type_check",
        ),
    )

    id: Mapped[uuid.UUID] = _uuid_pk()
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("app.users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    session_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("app.chat_sessions.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    run_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("app.query_runs.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    feedback_type: Mapped[str] = mapped_column(String(32), nullable=False)
    selected_suggestion: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    free_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class AgentSuggestionEvent(Base):
    """Suggestions generated for a run, plus optional clicked suggestion."""

    __tablename__ = "agent_suggestion_events"

    id: Mapped[uuid.UUID] = _uuid_pk()
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("app.users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    session_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("app.chat_sessions.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    run_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("app.query_runs.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    input_question: Mapped[str] = mapped_column(Text, nullable=False)
    intent: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    suggestions_generated: Mapped[List[Any]] = mapped_column(JSONB, nullable=False)
    selected_suggestion: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    result_status: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
