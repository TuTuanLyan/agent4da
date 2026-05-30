# Chat Sessions, Agent Visibility, and the Ask Workspace

Current-state reference for the chat-session feature on the Ask page. Code under
`app/backend` and `app/frontend` is the source of truth; this note tracks the
contracts that span both.

## Data model

`app.chat_sessions` (migration `0006_chat_sessions`, revises `0005_layer_stats`):

- `id` (uuid, pk), `user_id` (uuid, FK `app.users.id` ON DELETE CASCADE),
  `title` (nullable), `created_at`, `last_used_at`.

The same migration extends `app.query_runs` with:

- `session_id` (uuid, FK `app.chat_sessions.id` **ON DELETE SET NULL**),
- `turn_index` (int), `insights` (jsonb), `agent_engine` (text, default `legacy`).

Because the `session_id` FK is `SET NULL`, deleting a chat removes the thread but
keeps its runs in `query_runs`, so they remain visible in History.

Run the migration with the backend's alembic config; no other migration is
required.

## Backend APIs (`app/backend/agent/router.py`)

- `GET /agent/sessions` — the caller's sessions, most-recently-used first, each
  with `title`, timestamps, `run_count`, `last_question`, `last_status`.
- `POST /agent/sessions` — create an empty session, returns it (`201`).
- `GET /agent/sessions/current` — the most-recent session, creating one if none.
- `GET /agent/sessions/{id}/runs` — that session's turns as `AskResponse[]`,
  oldest first (ordered by `turn_index` then `created_at`).
- `DELETE /agent/sessions/{id}` — delete the chat (`204`); runs are kept.
- `POST /agent/ask` and `GET /agent/stream` accept an optional `session_id`.

Ownership is enforced everywhere a `session_id` is used: a session that does not
belong to the caller resolves to `404` (`resolve_chat_session` raises
`PermissionError`, which the router maps to `404`). When `session_id` is omitted,
`/ask` and `/stream` fall back to the caller's current session so context is
never silently lost.

A session is auto-titled from its first question (trimmed, ~80 chars) and its
title is never overwritten by later turns (`session_title_from_question`,
`update_session_after_run`). Follow-up turns prepend up to the last three
successful questions of the same session as prompt context; the persisted
`question` stays the user's original text.

`AskResponse` now also carries `insights`, `agent_engine`, `session_id`, and
`turn_index`.

## Agent engine visibility

`APP_AGENT_ENGINE` (default `legacy`) is surfaced read-only via
`GET /settings/system.agent_engine` and shown on the Settings "Model & Agent"
card as `Agent v2` when set to `v2`, otherwise `Agent (legacy)`. The engine is
environment-controlled; changing it requires recreating the backend container.
The runtime ships only the legacy LangGraph engine in this tree.

## Ask UX (`app/frontend/src/app/ask/page.tsx`)

A full-height two-pane workspace:

- Left: a chat sidebar with `New chat`, the session list, and a per-chat delete
  button (two-step inline confirm). On narrow screens the sidebar is a drawer
  toggled from the header.
- Right: the conversation timeline (user bubbles + assistant result blocks) above
  a pinned composer.

The active session is persisted to the URL (`?session=`) and to
`localStorage["agent4da.activeSessionId"]` for refresh recovery. Sending a
question inserts an immediate assistant placeholder with `aria-live` status text
(e.g. "Đang phân tích yêu cầu...") driven by the SSE step events, with
business-friendly step labels in `AgentStepper`.

`ChatResultBlock` renders results progressively: the short answer, key numbers,
and a "main insight" line first; the chart next; and the data table, SQL, and
verification notes behind expandable tabs (collapsed by default). When a run is
blocked, fails, or returns no rows, it offers clarification chips that re-ask in
the same session.

## Tests

`app/backend/tests/test_agent_sessions.py` covers session create/list, owned-
session enforcement (`404`), run loading (oldest-first + new response fields),
delete-keeps-runs, and auto-title behavior. It runs the real routers/services
over SQLite with small type shims (no live Postgres needed):

```bash
cd app/backend && python -m pytest tests/test_agent_sessions.py
```

Frontend checks:

```bash
cd app/frontend && npm run typecheck && npm run lint
```
