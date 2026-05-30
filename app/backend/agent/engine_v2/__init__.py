"""Agent v2 engine.

A faithful port of the standalone `code/api` LangGraph agent into the
`app/backend` runtime, adapted to the backend's reality:

- Trino catalog is `iceberg` (not `iceberg_catalog`); all generated SQL,
  guard rules, and corrections target `iceberg.gold.*`.
- LLM calls go through the OpenAI-compatible Groq client (the backend ships
  the `openai` SDK, not the `groq` SDK).
- Conversation follow-up context is process-local (single-replica MVP, like
  `agent.cancellation`); LangGraph checkpoint snapshots are persisted to
  `app.agent_checkpoint_snapshots` via SQLAlchemy instead of the langgraph
  Postgres saver (which is not installed).
- Ambiguous, blocked, empty, or off-source questions return contextual
  clarification suggestions instead of a hard unsupported message.

The public entrypoint is `agent.engine_v2.runner.run_agent_state_v2`.
"""

from __future__ import annotations
