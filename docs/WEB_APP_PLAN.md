# Agent4DA Web App - End-to-End Implementation Plan

This plan turns the existing CLI LangGraph agent (`code/agent/`) and the Gold
semantic layer into a complete web application. The plan is grounded in the
current repository: it reuses `code/agent/graph/sql_graph.py`, the
`iceberg_catalog.metadata.semantic_table_catalog` /
`iceberg_catalog.metadata.semantic_column_catalog` tables, the `envs/*.env`
secret pattern, the existing `data_network` docker network, and the Makefile
compose convention.

## 0. Decisions Locked In

- Product name: Agent4DA Analytics Console - a working dashboard, not a
  landing page.
- Information architecture (sidebar, top to bottom): Ask, History, Catalog,
  Pipelines, Settings. Ask is the default route.
- Topbar: project name, three live status pills (Trino / Spark / Airflow),
  theme toggle (Light / Dark / System), user avatar with sign-out.
- Scope V1: Text-to-SQL Ask screen with summarize, query history with
  favorites, read-only semantic catalog, pipeline health console, settings.
  Standalone dashboards screen is intentionally dropped; a Quick Stats strip
  on Ask covers the "what is going on right now" need from Gold summary
  tables.
- Frontend: Next.js 14 (App Router) + TypeScript + Tailwind + shadcn/ui +
  Recharts. Theming via `next-themes`. Icons from `lucide-react`. Tables from
  `@tanstack/react-table`. Data fetching via `@tanstack/react-query`.
- Backend: FastAPI (Python 3.12) wrapping the existing LangGraph agent. Same
  conda env (`agent4daenv`) so we reuse `openai`, `langgraph`, `trino`,
  `fastapi`, `uvicorn` already pinned in `agent4da.env.yml`.
- Agent: extend `code/agent/graph/sql_graph.py` with a sixth node
  `summarize` so the Ask screen can render a natural-language Answer tab.
  Summarize is on by default and can be skipped per request to save a Groq
  call.
- Auth: JWT (FastAPI). Single role `user` for everyone, hidden role `admin`
  for triggering DAG runs from Pipelines. Catalog is read-only for all
  users in V1.
- Persistence for app data: PostgreSQL (the existing `postgres-db` container,
  new schema `app`). Users, preferences, query history, favorites, sample
  questions, and cached layer stats live here. Gold data stays in Iceberg
  via Trino.
- Packaging: New `docker-compose.app.yml` joined to `data_network`, plus
  `make app-up / app-down / app-logs` targets.
- Design tokens (Tailwind + shadcn theme variables) are locked from the
  proposal: see Section 6 for the full palette. Border radius 8px, font
  Inter, no gradients, no neon, dark mode uses slate/charcoal, not pure
  black.
- No production deploy in scope yet; the plan targets local docker-compose,
  but everything is structured so a later cloud deploy is a swap of compose
  files and env values.

## 1. Repository Layout Additions

```text
app/
  backend/
    pyproject.toml
    Dockerfile
    api/
      __init__.py
      main.py                # FastAPI app factory, CORS, routers, OpenAPI tags
      deps.py                # DB session, current_user, role guards
      settings.py            # pydantic-settings, reads envs/app.env + groq.env
      logging.py
    auth/
      router.py              # /auth/login, /auth/me, /auth/refresh
      models.py              # SQLAlchemy User
      service.py             # password hash (argon2), JWT issue/verify
      schemas.py             # Pydantic
    agent/
      router.py              # /agent/ask, /agent/stream (SSE), /agent/stop,
                             # /agent/runs/{id}, /agent/runs/{id}/export.csv,
                             # /agent/sample-questions
      service.py             # wraps code.agent.graph.sql_graph + summarize
      cancellation.py        # run_id -> asyncio.Task + Trino query_id registry
      summarize.py           # post-execute LLM summary node (Vietnamese/English)
      schemas.py
    catalog/
      router.py              # READ-ONLY: /catalog/tables, /catalog/tables/{name},
                             # /catalog/columns, /catalog/search?q=
      service.py             # Trino reads + table_kind classifier (fact/dim/summary)
      schemas.py
    history/
      router.py              # /history, /history/{id},
                             # POST /history/{id}/favorite, DELETE same
      models.py              # QueryRun (incl. is_favorite, summary_text, chart_type)
      service.py
    pipelines/
      router.py              # /pipelines (rollup), /pipelines/{dag}/runs,
                             # /pipelines/{dag}/runs/{run_id}/logs,
                             # POST /pipelines/{dag}/trigger (admin)
      airflow_client.py      # Airflow REST client + log streaming
      service.py             # combines DAG state with cached layer row counts
      schemas.py
    settings/
      router.py              # /settings/me (GET/PUT), /settings/system (GET)
      service.py             # user prefs + masked system status
      schemas.py
    ops/
      health.py              # /ops/health: Trino + Spark + Airflow combined probe
      minio_client.py        # MinIO bucket size/object count (used by snapshot)
      scheduler.py           # apscheduler jobs: refresh layer_stats + health cache
    quickstats/
      router.py              # /quickstats: today revenue / MTD events / top brand
      queries.py             # canned SQL against daily_event_summary, daily_brand_summary
    db/
      base.py                # SQLAlchemy Base, engine, sessionmaker
      models.py              # re-exports
      migrations/            # Alembic
    chart/
      heuristics.py          # auto-chart inference from result columns
      schemas.py
  frontend/
    package.json
    Dockerfile
    next.config.mjs
    tailwind.config.ts                 # design tokens from Section 6
    tsconfig.json
    src/
      app/
        layout.tsx                     # Sidebar + Topbar shell, ThemeProvider
        page.tsx                       # redirects to /ask
        login/page.tsx
        ask/page.tsx                   # Ask (default screen)
        history/page.tsx
        history/[runId]/page.tsx       # re-open old result, read-only
        catalog/page.tsx               # tables list with badges + search
        catalog/[tableName]/page.tsx   # columns with copy buttons
        pipelines/page.tsx             # Bronze/Silver/Gold/Metadata cards
        pipelines/[dagId]/runs/[runId]/page.tsx   # run detail + logs
        settings/page.tsx              # theme, model, chart default, system status
      components/
        ui/                            # shadcn primitives
        layout/
          Sidebar.tsx                  # 5 nav items + active state
          Topbar.tsx                   # project name + status pills + theme + user
          StatusPill.tsx               # green/yellow/red dot + label
          ThemeToggle.tsx              # light/dark/system
        ask/
          QuestionInput.tsx            # textarea + run button + sample chips
          SampleChips.tsx              # server-provided sample questions
          AgentStepper.tsx             # 5 steps: Load -> Generate -> Validate -> Execute -> Summarize
          QuickStatsStrip.tsx          # today revenue / MTD events / top brand
          ResultTabs.tsx               # Answer / Chart / Table / SQL
          AnswerPanel.tsx              # LLM summary text + small key numbers
          ChartPanel.tsx               # type selector (bar/line/pie/table) + Recharts
          TablePanel.tsx               # TanStack Table with sort/filter/search/paginate
          SqlPanel.tsx                 # syntax-highlighted SQL + guard status + copy
          ActionBar.tsx                # Run, Stop, Retry, Copy SQL, Export CSV, Save
        history/
          HistoryTable.tsx             # status, question, latency, rows, favorite
          FavoriteToggle.tsx
        catalog/
          TableList.tsx                # search + kind badge (fact/dim/summary)
          ColumnList.tsx               # name, type, meaning, business_terms, copy
          KindBadge.tsx
        pipelines/
          PipelineCard.tsx             # DAG status tile
          RunsTable.tsx                # last N runs
          LogViewer.tsx                # paged log stream
          RowCountTile.tsx             # layer_stats snapshot value
        settings/
          ThemeSection.tsx
          ModelSection.tsx             # provider locked, model whitelist, temp read-only
          ConnectionStatusSection.tsx  # Trino/Spark/Airflow/Groq configured-or-missing
          DefaultsSection.tsx          # chart default, export delimiter
        states/
          Empty.tsx                    # icon + headline + CTA
          Loading.tsx                  # skeleton variants
          ErrorState.tsx               # title + reason + Retry button
      lib/
        api.ts                         # fetch wrapper, JWT refresh, abort signal
        auth.ts                        # client-side session
        chart-pick.ts                  # mirrors backend heuristics
        theme-tokens.ts                # exported token map for charts
        types.ts                       # generated from FastAPI OpenAPI
      hooks/
        useAgentStream.ts              # SSE/fetch-stream consumer, returns step + result
        useHealth.ts                   # /ops/health poll (30s) for topbar pills
        usePrefs.ts                    # /settings/me load + optimistic update
docker-compose.app.yml
envs/app.env                            # gitignored, app-only secrets
docs/WEB_APP_PLAN.md                    # this file
```

Rationale: a single `app/` root keeps the web app self-contained and mirrors
the `code/` style. The backend imports the existing agent package via a path
mount inside the Docker image (`/opt/project/code` -> `PYTHONPATH`), exactly
the way DAGs already pick up `code/spark` and `code/airflow`.

## 2. Phased Delivery

The plan is 7 phases. Each phase ends with something runnable so progress is
visible.

### Phase 1 - Bootstrap, design tokens, layout shell (Day 1-2)

1. Create `app/`, `envs/app.env.example`, and `docker-compose.app.yml` (Postgres
   reused, only `app-api` and `app-web` are new).
2. Add `APP_*` variables to `docs/ENV_SETUP.md`:
   - `APP_JWT_SECRET`, `APP_JWT_ALG=HS256`, `APP_ACCESS_TOKEN_TTL_MIN=60`,
     `APP_REFRESH_TOKEN_TTL_DAYS=14`,
   - `APP_DB_URL=postgresql+psycopg://bigdata:...@postgres-db:5432/agent4da`
     (schema `app`),
   - `APP_TRINO_HOST=trino`, `APP_TRINO_PORT=8080`,
   - `APP_AIRFLOW_BASE_URL=http://airflow:8080`,
     `APP_AIRFLOW_USER=...`, `APP_AIRFLOW_PASSWORD=...`,
   - `APP_MINIO_ENDPOINT=http://minio:9000`, reuse `MINIO_ACCESS_KEY`/
     `MINIO_SECRET_KEY`,
   - `APP_GROQ_MODEL_WHITELIST=llama-3.3-70b-versatile,llama-3.1-8b-instant`,
   - `APP_ALLOW_TEMPERATURE_OVERRIDE=false`,
   - `APP_CORS_ORIGINS=http://localhost:3000`.
3. Scaffold FastAPI (`app/backend/api/main.py`) with `/healthz` and OpenAPI
   under `/openapi.json`.
4. Scaffold Next.js with shadcn init + `next-themes`, install
   `@tanstack/react-query`, `@tanstack/react-table`, `recharts`, `lucide-react`.
5. Wire the layout shell:
   - `Sidebar.tsx` with five entries (Ask, History, Catalog, Pipelines,
     Settings) and a "v0.1" footer,
   - `Topbar.tsx` with project name, three `StatusPill`s (initially all gray
     "unknown"), `ThemeToggle`, and a placeholder avatar,
   - `ThemeProvider` reading `next-themes` (`light` / `dark` / `system`).
6. Land the design tokens from Section 6 into `tailwind.config.ts` and
   `app/globals.css` as CSS variables on `:root` and `[data-theme="dark"]`.
   shadcn primitives must read from these variables so a single theme toggle
   flips the whole app.
7. Confirm `make app-up` brings both containers up alongside `make trino-up`,
   `make postgre-up`, etc.

Exit check: `curl localhost:8083/healthz` returns 200; `localhost:3000` shows
the 5-tab shell with Ask selected, theme toggle works, status pills render
(even if still gray).

### Phase 2 - Auth, users, DB migrations, preferences (Day 3-4)

Role model simplified for V1: a single `user` role for everyone, and a hidden
`admin` role used only by the "Trigger DAG" button in Pipelines. Catalog has
no edit mode, so no editor role is needed.

1. Add SQLAlchemy 2.x + Alembic + psycopg in `pyproject.toml`. Create
   migration `0001_init` that creates `app` schema with:
   - `users(id uuid pk, email citext unique, password_hash text, role
     text check in ('user','admin') default 'user', created_at timestamptz)`,
   - `refresh_tokens(id uuid pk, user_id fk, jti text unique, expires_at
     timestamptz, revoked_at timestamptz)`,
   - `user_preferences(user_id uuid pk fk users.id, theme text default 'system',
     default_chart_type text default 'auto', default_model text,
     preferred_language text default 'vi', export_delimiter text default ',',
     updated_at timestamptz default now())`.
2. Implement password hashing with argon2 (`argon2-cffi`, already pinned).
3. Endpoints: `POST /auth/login`, `POST /auth/refresh`, `POST /auth/logout`,
   `GET /auth/me`. Issue short-lived access JWT in body, refresh JWT in HttpOnly
   `secure` cookie.
4. Role guard dependency: `require_role("admin")` - used by the DAG trigger
   endpoint only.
5. Seed script `app/backend/scripts/seed_admin.py` that reads
   `APP_BOOTSTRAP_ADMIN_EMAIL`/`APP_BOOTSTRAP_ADMIN_PASSWORD` and inserts a
   single admin user. Idempotent.
6. Auto-create a `user_preferences` row on first login if missing.
7. Frontend: `/login` form posting to `/auth/login`; `lib/auth.ts` stores
   access token in memory and refresh via cookie; route guard wrapper that
   redirects unauthenticated traffic to `/login`. `usePrefs.ts` hydrates the
   ThemeProvider from `/settings/me`.

Exit check: log in, see your email at `GET /auth/me`, the theme toggle now
persists across reloads via the server, and a non-admin user does not see
the "Trigger" button on Pipelines.

### Phase 3 - Ask screen: agent, summarize, stop, export (Day 5-9)

This is the centerpiece. The Ask screen must show the agent progress as 5
steps, expose 4 result tabs, and support Run / Stop / Retry / Copy SQL /
Export CSV / Save.

Backend:

1. Extend `code/agent/graph/sql_graph.py` with a new node `summarize` after
   `execute_sql`. The node calls Groq with a short prompt that takes the
   user question, the generated SQL, and the first 20 rows, and returns a
   2-4 sentence Vietnamese/English insight plus a `key_numbers` list. The
   node respects `AGENT_SUMMARIZE` env (default true) and skips quietly if
   false. The CLI entrypoint stays backwards compatible.
2. `app/backend/agent/service.py` imports the graph by adding `code/agent`
   and its subdirs to `sys.path` at startup. Existing CLI keeps working.
3. `POST /agent/ask` (auth required): body
   `{question, summarize?, chart_type?}` -> response
   `{run_id, question, generated_sql, guard_status, columns, rows,
   row_count, error, latency_ms, summary, key_numbers, chart_suggestion}`.
   Persists a row in `app.query_runs` (see Phase 4 schema).
4. `GET /agent/stream?question=...` (server-sent events). Emits one event
   per LangGraph node completion:
   ```text
   event: step  data: {"step":"load_metadata","status":"ok"}
   event: step  data: {"step":"generate_sql","status":"ok","sql":"..."}
   event: step  data: {"step":"guard_sql","status":"ok"}
   event: step  data: {"step":"execute_sql","status":"ok","row_count":42}
   event: step  data: {"step":"summarize","status":"ok"}
   event: result data: { ...full payload... }
   ```
5. `POST /agent/stop` body `{run_id}`: looks up the asyncio task in
   `agent/cancellation.py` and the Trino query_id, calls Trino
   `DELETE /v1/query/{query_id}`, cancels the task, marks the run
   `status="stopped"`. 204 on success.
6. `GET /agent/runs/{run_id}/export.csv` streams the cached result rows as
   CSV with delimiter from `user_preferences.export_delimiter`. Sets
   `Content-Disposition: attachment; filename="agent4da_<run_id>.csv"`.
7. `GET /agent/sample-questions`: returns rows from `app.sample_questions`
   (`label`, `question`, `sort_order`). Seed it with the four examples
   from the proposal: "doanh thu theo ngay", "top brand", "category
   conversion", "session revenue".
8. Re-run safety: server-side timeout default 30s, wrap Trino exceptions,
   never echo Groq raw errors beyond a sanitized message. SQL safety
   extensions in Section 3.1.

Frontend (`/ask` page):

1. Layout matches the proposal:
   ```text
   Question input (Textarea + Run / Stop buttons + sample chips)
   AgentStepper (5 steps)
   QuickStatsStrip (today revenue / MTD events / top brand MTD)
   ResultTabs (Answer | Chart | Table | SQL)
   ActionBar (Run, Stop, Retry, Copy SQL, Export CSV, Save)
   ```
2. `QuestionInput.tsx`: shadcn `Textarea`, Cmd/Ctrl+Enter to run, character
   counter, language hint (vi/en) auto-detected.
3. `SampleChips.tsx`: loads `/agent/sample-questions` once on mount, renders
   chips, click fills the input but does not auto-run.
4. `AgentStepper.tsx`: 5 nodes with icons - `Database`, `FileCode`,
   `ShieldCheck`, `Play`, `Sparkles`. Each shows pending / running /
   ok / error with the accent color from tokens.
5. `useAgentStream.ts` uses `fetch` with `ReadableStream` + `AbortController`
   (not EventSource) so the Authorization header is sent properly. Yields
   step events; final `result` event hydrates the tabs.
6. `ResultTabs.tsx`:
   - **Answer**: `AnswerPanel.tsx` shows `summary` text in markdown plus
     `key_numbers` as small cards. Empty state when summarize was off.
   - **Chart**: `ChartPanel.tsx` shows a type selector (`bar | line | pie |
     table`) with the auto-chosen type pre-selected. Uses Recharts with
     colors pulled from `lib/theme-tokens.ts`.
   - **Table**: `TablePanel.tsx` via TanStack Table - sort, column filter,
     global search, paginate 50/page, click-to-copy a cell.
   - **SQL**: `SqlPanel.tsx` syntax-highlighted SQL (Shiki) + guard status
     badge (`PASS` / `BLOCKED reason`) + copy button.
7. `ActionBar.tsx`:
   - Run: same as primary submit.
   - Stop: visible only while streaming; calls `/agent/stop`.
   - Retry: re-submits the same question (no chat-history concept; the
     question text is editable so users can tweak then retry).
   - Copy SQL: clipboard write.
   - Export CSV: window.location to
     `/agent/runs/{run_id}/export.csv?token=...` (one-time JWT in query for
     download, validated server-side).
   - Save: calls `POST /history/{run_id}/favorite`.
8. Wire empty / loading / error states via the shared `states/` components.
   Errors always show a Retry button.

Exit check: ask "Doanh thu theo ngay trong thang 1 nam 2020"; the stepper
walks through all 5 steps in order; Answer tab shows a Vietnamese summary;
Chart shows a line; Table shows daily rows; SQL tab shows guarded SQL.
Hitting Stop mid-execute kills the Trino query (verifiable in Trino UI).
Export CSV downloads the same rows.

### Phase 4 - History screen with favorites (Day 10)

Backend:

1. Migration `0002`: `query_runs(id uuid pk, user_id uuid fk users.id,
   question text, generated_sql text, guard_status text check in
   ('pass','blocked','error'), row_count int, latency_ms int, error text,
   summary_text text, chart_type text, is_favorite bool default false,
   trino_query_id text, result_json_uri text, status text check in
   ('success','failed','stopped'), created_at timestamptz default now())`.
   Result rows are too large for an inline JSONB column; they're written
   to MinIO under `s3a://app/query_results/<run_id>.json.gz` and the URI
   stored - read back when the user re-opens an old run.
2. Endpoints:
   - `GET /history?from=&to=&status=&favorite=&q=&page=` paged list,
   - `GET /history/{run_id}` returns the same shape as `/agent/ask`,
     re-hydrating rows from MinIO,
   - `POST /history/{run_id}/favorite` and `DELETE` toggle.
3. The agent service writes one `query_runs` row per invocation. Result
   payload uploaded async (do not block the response).

Frontend (`/history` page):

1. `HistoryTable.tsx`: columns = star toggle, status badge
   (success/failed/stopped/blocked), question (truncated, hover shows
   full), latency, row count, created_at, action menu (Re-run, Copy SQL,
   Open).
2. Filters above the table: date range, status multi-select, favorites
   only, search by question text.
3. Click a row -> `/history/[runId]` opens a read-only Ask layout (no Run
   button) with all 4 tabs hydrated and a prominent "Re-run this question"
   button that copies the question to `/ask`.
4. Sidebar favorites: a small section under the History entry that lists
   the latest 5 favorites for one-click re-run; populated via the same
   endpoint with `favorite=true&limit=5`.

Exit check: every Ask invocation appears in `/history` instantly; toggling
a star shows it in the sidebar shortlist; opening an old run re-hydrates
the same 4 tabs without re-running the query.

### Phase 5 - Catalog (read-only semantic browser) + Quick Stats (Day 11-12)

The proposal drops the editor for V1. Catalog is purely read. The Quick
Stats strip on Ask is the only "dashboard"-style surface and lives here
because it shares the same Gold summary tables.

Backend (Catalog):

1. `app/backend/catalog/service.py` runs the same Trino reads as
   `code/agent/services/metadata_service.py` but exposes them as HTTP:
   - `GET /catalog/tables` -> list rows from
     `iceberg.metadata.semantic_table_catalog` where
     `is_agent_visible=true`, with a derived `kind` field
     (`fact | dimension | summary | semantic`) computed by
     `_classify(table_name)`:
       - starts with `fact_` -> fact,
       - starts with `dim_` -> dimension,
       - starts with `daily_` or ends with `_summary` -> summary,
       - everything else -> semantic.
   - `GET /catalog/tables/{table_name}` returns the table row + its
     columns (`semantic_column_catalog`),
   - `GET /catalog/columns?table_name=&q=` filtered list,
   - `GET /catalog/search?q=` searches both `table_name`/`display_name`
     and `column_name`/`business_terms` and returns top 20 hits with
     match snippets.
2. Cache `/catalog/tables` for 5 minutes (in-process LRU) - metadata
   changes only via the existing Spark `gold_build_metadata` DAG.

Backend (Quick Stats):

3. `app/backend/quickstats/queries.py` declares three small parameterized
   SQL constants against Gold summaries:
   - `TODAY_REVENUE`: `SELECT total_revenue FROM iceberg.gold.daily_event_summary
     WHERE event_date = current_date`,
   - `MTD_EVENTS`: `SELECT SUM(total_events) FROM iceberg.gold.daily_event_summary
     WHERE event_date >= date_trunc('month', current_date)`,
   - `MTD_TOP_BRAND`: `SELECT brand, SUM(revenue) AS revenue FROM
     iceberg.gold.daily_brand_summary WHERE event_date >=
     date_trunc('month', current_date) GROUP BY brand ORDER BY revenue DESC
     LIMIT 1`.
4. `GET /quickstats` returns all three with `cached_at`; results cached
   for 60s.

Frontend (`/catalog`):

1. `/catalog` page: search bar + `TableList.tsx` with rows = `table_name`,
   `display_name`, `KindBadge` (fact=teal, dim=blue, summary=violet,
   semantic=gray), `purpose` (truncated), copy-table-name button.
2. `/catalog/[tableName]`: header card (purpose, grain, use_for, query_notes)
   + `ColumnList.tsx` = columns table with name (copy button), data_type
   chip, meaning, business_terms, example_usage in a `<code>` block.
3. Search box debounced 200ms calling `/catalog/search`.
4. Empty state when no rows match; never blank.

Frontend (Quick Stats on Ask):

5. `QuickStatsStrip.tsx` shows three cards (Today revenue, MTD events, Top
   brand MTD) above the question input. Skeleton while loading, never
   error-blocking; if Trino is down it collapses to a neutral notice.

Exit check: open Catalog, search "revenue" and see hits across multiple
tables/columns; click a fact table and see its columns with `business_terms`
hinted in the example column; reload Ask and the three Quick Stats numbers
match a Trino query you can re-run by hand.

### Phase 6 - Pipelines screen + Airflow integration (Day 13-15)

Backend:

1. `app/backend/pipelines/airflow_client.py` wraps Airflow REST API
   (`/api/v1/dags`, `/dags/{id}/dagRuns`,
   `/dags/{id}/dagRuns/{run_id}/taskInstances`,
   `/dags/{id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try}`)
   using basic auth from `APP_AIRFLOW_USER` / `_PASSWORD`. Supports JWT
   variant if `APP_AIRFLOW_AUTH=jwt`.
2. `GET /pipelines` returns a rollup for the four DAGs
   (`bronze_pipeline`, `silver_pipeline`, `gold_pipeline`,
   `gold_metadata_pipeline`):
   ```text
   { dag_id, schedule, last_run_at, last_run_status, last_duration_sec,
     next_run_at, row_count_after_last_run, layer }
   ```
   `row_count_after_last_run` is joined from `app.layer_stats` (see 3.7).
3. `GET /pipelines/{dag_id}/runs?limit=20` returns recent runs.
4. `GET /pipelines/{dag_id}/runs/{run_id}/logs?task=&try=` proxies Airflow
   logs, returns text with a max size cap of 256 KB, suggests truncation
   when bigger.
5. `POST /pipelines/{dag_id}/trigger` (admin only) triggers a manual run
   with optional conf. Audited to `app.pipeline_trigger_audit`.
6. `GET /pipelines/debug-command?dag={dag_id}` returns a copy-paste shell
   line equivalent to what the existing `script/spark/submit_*.sh`
   scripts run - lets a power user reproduce a failure on the host.

Frontend (`/pipelines`):

1. Page shows 4 `PipelineCard.tsx` tiles in order Bronze, Silver, Gold,
   Metadata. Each card:
   - status dot (green/yellow/red) + last-run human time ("3 minutes ago"),
   - duration, run id, schedule,
   - one `RowCountTile` showing row count for the layer after that run,
   - actions: Open runs, Open logs (latest task), Copy debug command,
     Trigger (admin only).
2. `/pipelines/[dagId]/runs/[runId]`: `RunsTable.tsx` + `LogViewer.tsx`
   (auto-scroll, follow-tail toggle, line wrap toggle, copy-all).
3. Polling: TanStack Query with 15s stale time on `/pipelines`, 5s on the
   run detail when status is running.

Exit check: trigger `bronze_pipeline` as admin from the UI; the card flips
to "running", a row appears in `/pipelines/bronze_pipeline/runs/<id>`, the
log viewer streams lines, and on success the row count tile updates after
the next `layer_stats` refresh.

### Phase 7 - Settings, Topbar status pills, snapshot scheduler (Day 16-17)

Backend:

1. `app/backend/ops/health.py` - single endpoint `GET /ops/health` that
   probes Trino (`/v1/info` + `SELECT 1`), Spark (Spark master `/json/`
   or `/api/v1/applications`), Airflow (`/api/v1/health`), Groq
   (configured-or-missing only, no live call). Returns:
   ```text
   { trino: {status: 'ok'|'degraded'|'down', version, latency_ms},
     spark: {status, workers, latency_ms},
     airflow: {status, latency_ms},
     groq:   {status: 'configured'|'missing'},
     checked_at }
   ```
   Cached for 15s in-process so topbar polling does not hammer services.
2. `app/backend/ops/scheduler.py` boots an `AsyncIOScheduler` on startup:
   - every 5 min: refresh `app.layer_stats` by running cheap `COUNT(*)`
     against Iceberg gold tables and Hive-style Parquet counts for
     Bronze/Silver via Trino (`SELECT COUNT(*) FROM iceberg.bronze.events`
     style with Trino's `hive` catalog if registered, otherwise via a
     small Spark helper script triggered by the existing
     `silver_pipeline` post-hook),
   - every 5 min: refresh per-bucket size/object count from MinIO into
     `app.layer_stats`,
   - every 30s: refresh the cached health probe.
3. `GET /settings/me` / `PUT /settings/me` for user preferences (theme,
   default_chart_type, default_model, preferred_language,
   export_delimiter). Validates `default_model` against
   `APP_GROQ_MODEL_WHITELIST`.
4. `GET /settings/system` returns a redacted view of the environment so
   the UI can show "configured" or "missing":
   ```text
   { trino: 'configured', groq: 'configured', airflow: 'missing',
     minio: 'configured', allow_temperature_override: false,
     model_whitelist: ['llama-3.3-70b-versatile', ...] }
   ```
   No secret values ever leave the backend.

Frontend:

1. `Topbar.tsx` consumes `useHealth()` and renders three `StatusPill`s.
   Polling: 30s when tab is focused, paused when hidden.
2. `/settings` page with sections:
   - **Theme**: radio Light/Dark/System (writes to `user_preferences.theme`).
   - **Model**: provider chip locked to `Groq`, model dropdown from
     `model_whitelist`, temperature read-only `0` (gated by
     `APP_ALLOW_TEMPERATURE_OVERRIDE`).
   - **Connection status**: 4 rows (Trino, Spark, Airflow, Groq) reading
     `/settings/system` and `/ops/health` - green check or red x; never
     prints secret values.
   - **Defaults**: default chart type (`auto/bar/line/pie/table`), export
     CSV delimiter, preferred language (vi/en).
3. Settings writes call `PUT /settings/me` and optimistically update via
   `usePrefs`. The ThemeProvider listens to the same store so it flips
   instantly.

Exit check: change Light->Dark in Settings, reload, theme persists;
unplug Airflow and within 30s the Airflow topbar pill turns red while
Trino/Spark stay green; the Settings system status flips Airflow to
"missing".

## 3. Cross-Cutting Concerns

### 3.1 SQL safety (extending the existing guard)

The current `guard_sql_node.py` rejects non-`SELECT` and forbidden keywords.
For the web app, add three more guards in
`app/backend/agent/service.py` after the LangGraph result:

- Reject if `state["generated_sql"]` references a table not present in
  `semantic_table_catalog` with `is_agent_visible=true`. Implementation:
  parse with `sqlglot` (`pip install sqlglot`) and walk `Table` nodes.
- Reject queries without a `LIMIT` clause on `fact_*` tables; auto-append
  `LIMIT 10000` if missing, log a warning to history.
- Enforce a hard Trino query timeout via session property
  `query_max_execution_time = '30s'`.

The Sql tab in the UI shows the final guard status (`PASS`, `BLOCKED:
reason`, `AUTO-LIMITED`) so users see why a query was modified or rejected.

These guards are added in the API layer, so the existing `code/agent/`
package stays a pure MVP - the only change to `code/agent/` is the optional
`summarize` node which respects `AGENT_SUMMARIZE`.

### 3.1b Cancellation (Stop button)

`app/backend/agent/cancellation.py` keeps an in-memory registry:

```text
run_id -> { task: asyncio.Task, trino_query_id: str | None,
            started_at: datetime, user_id: uuid }
```

Flow:

1. `execute_sql_node` is wrapped so the moment Trino assigns a query id
   (via `cursor.query_id` from `trino-python-client`), it's stored.
2. `POST /agent/stop` looks up the entry, calls
   `DELETE http://trino:8080/v1/query/{query_id}` to kill the query
   server-side, then `task.cancel()` on the asyncio task.
3. The agent service catches `CancelledError`, marks the row
   `status='stopped'`, and SSE emits `event: stopped`.

This actually frees Trino resources - cancelling only the Python coroutine
would leave the query running on the cluster.

### 3.2 Auto-chart heuristics (`app/backend/chart/heuristics.py`)

```text
inputs: list of column names + dtypes + first-N rows
rules (first match wins):
  1. 1 date/timestamp col + 1 numeric col -> line, x=date, y=numeric
  2. 1 date col + multiple numeric -> stacked line
  3. 1 categorical (<=20 distinct) + 1 numeric -> bar, sorted desc
  4. 1 categorical (<=8 distinct) + 1 numeric sharing a "part-of" relation
     (column name in {'share','pct','ratio','percent','rate'}) -> pie
  5. 2 numeric -> scatter
  6. otherwise -> table only
output: {chart_type, x, y, series?} | null
```

The Chart tab in the UI starts with the auto-chosen type but lets the user
override to bar/line/pie/table on the fly; the override is stored in
`query_runs.chart_type` so re-opening from History keeps the choice.

The same rules are duplicated in `frontend/src/lib/chart-pick.ts` so the UI
can re-derive a chart if the user clears the override.

### 3.2b Summarize prompt

`app/backend/agent/summarize.py` calls Groq with a structured prompt and
asks the model to return JSON of shape:

```text
{ "summary": "<=4 sentences, language matches user question>",
  "key_numbers": [{"label": "...", "value": "...", "delta": "..." }] }
```

Inputs sent to the LLM:
- the original user question,
- the generated SQL,
- the column names and the first 20 result rows.

Server-side strips the response and falls back to plain text if JSON parse
fails. Toggle by `AGENT_SUMMARIZE` env (default true) and per-request
`summarize: false` flag.

### 3.3 Auth and CORS

- FastAPI middleware: `CORSMiddleware(allow_origins=APP_CORS_ORIGINS,
  allow_credentials=True, allow_methods=["GET","POST","PUT","DELETE"])`.
- Refresh tokens stored server-side (`refresh_tokens` table) with `jti`
  so logout actually revokes. Access tokens are stateless.
- Frontend never stores the access token in localStorage; it lives in a React
  context and is refreshed via the HttpOnly cookie on app load and on 401.

### 3.4 Configuration and secrets

- New file `envs/app.env` (gitignored), template at `envs/app.env.example`
  (tracked).
- `app/backend/api/settings.py` uses `pydantic-settings` with `env_file`
  list: `["/envs/app.env", "/envs/groq.env"]` inside the container.
- The container mounts `./envs:/envs:ro` like other services already do.
- No secret-bearing config files baked into the image.

### 3.5 Observability

- Backend: `structlog` JSON logs to stdout, request-id middleware.
- Backend: `/metrics` endpoint (Prometheus client) exposing
  `agent_requests_total`, `agent_latency_seconds`, `agent_errors_total`,
  `trino_query_seconds`, `guard_reject_total`, `agent_summarize_seconds`,
  `agent_stop_total`, `health_probe_seconds{service=...}`,
  `layer_stats_refresh_total`.
- Frontend: a single error boundary, plus toast on API errors via shadcn
  `<Toaster>`.
- Optional later: drop a `monitoring/` Prometheus + Grafana compose file -
  the directory is already mentioned in `PROJECT.md` as planned.

### 3.6 Snapshot scheduler

`ops/scheduler.py` boots `AsyncIOScheduler` jobs on FastAPI startup:

| Job | Cadence | Purpose |
| --- | --- | --- |
| `refresh_health` | 30s | Probe Trino/Spark/Airflow, cache result for `/ops/health`. |
| `refresh_layer_stats` | 5m | Update `app.layer_stats` for Bronze/Silver/Gold row counts and byte sizes. |
| `refresh_quickstats` | 60s | Pre-warm the `/quickstats` cache (best-effort). |
| `cleanup_query_results` | daily 03:00 | Delete `s3a://app/query_results/<run_id>.json.gz` older than 30 days. |

Jobs are noop-safe if dependencies are down; they log and retry next tick.

### 3.7 Testing

Backend (`pytest` + `httpx.AsyncClient`):
- Unit: chart heuristics, sqlglot guard, JWT issue/verify, password hashing.
- Integration: spin up an ephemeral Postgres in CI; mock the Trino client
  with `respx` for HTTP, or use a tiny SQLite-backed in-memory adapter for
  history tests.
- Contract: snapshot `openapi.json` so the frontend's generated types stay
  in sync.

Frontend (`vitest` + `@testing-library/react`):
- Components: `ChartPanel` rule selection, `SqlPanel` copy behavior,
  `AgentStepper` state machine, `KindBadge` mapping, `FavoriteToggle`.
- E2E (Playwright, optional Day 18+):
  - login -> Ask "Doanh thu theo ngay thang 1/2020" -> see 5 steps + chart,
  - Stop mid-execute -> confirm Trino query gone,
  - History -> star a run -> appears in sidebar -> re-run,
  - Catalog -> search "doanh thu" -> click a column -> copy column name,
  - Settings -> Dark -> reload -> still dark,
  - Pipelines -> admin trigger -> log viewer streams.

The minimum CI command set, dropped into `.github/workflows/app.yml`:

```bash
# backend
python -m compileall app/backend
pytest app/backend/tests -q

# frontend
cd app/frontend && npm ci && npm run lint && npm run test -- --run && npm run build
```

These pair with the existing project checklist:

```bash
python -m compileall code
python -m compileall code/agent
```

### 3.8 Docker and compose wiring

`app/backend/Dockerfile` is multi-stage:

```text
FROM python:3.12-slim AS base
WORKDIR /opt/project
# install build deps; copy pyproject + lock; pip install --no-cache
# copy app/backend and code/agent (mounted PYTHONPATH)
ENV PYTHONPATH=/opt/project/code/agent:/opt/project/app/backend
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

`app/frontend/Dockerfile` builds the Next.js app (`next build`) and runs
`next start` on port 3000.

`docker-compose.app.yml`:

```yaml
services:
  app-api:
    build: ./app/backend
    container_name: app-api
    env_file: [./envs/app.env, ./envs/groq.env, ./envs/postgre.env, ./envs/minio.env]
    ports: ["8083:8000"]
    volumes:
      - ./code:/opt/project/code:ro
      - ./envs:/envs:ro
    depends_on: { postgres: { condition: service_healthy }, trino: { condition: service_healthy } }
    networks: [data_network]

  app-web:
    build: ./app/frontend
    container_name: app-web
    env_file: [./envs/app.env]
    environment:
      NEXT_PUBLIC_API_BASE_URL: http://localhost:8083
    ports: ["3000:3000"]
    depends_on: [app-api]
    networks: [data_network]

networks:
  data_network:
    external: true
    name: data_network
```

Makefile gains:

```make
SERVICES := kafka spark minio postgre airflow trino app
COMPOSE_app := docker-compose.app.yml
```

so `make app-up`, `make app-down`, `make app-logs`, and the `ps` rollup work
automatically.

### 3.9 Following project conventions

- Do NOT add `spark.jars.packages`; the Spark side keeps using local jars
  from `/opt/project/jars`. The metadata publish DAG hands off to the
  existing `gold_metadata_pipeline`, which already follows this rule.
- All identifiers in the backend pass through helpers analogous to
  `gold.identifiers`. Reuse that module from the backend rather than
  re-implementing string interpolation.
- Logs use `print(..., flush=True)` in DAG-adjacent code, `structlog` in the
  pure API code.
- ASCII for new files; existing Vietnamese docs stay Vietnamese.
- Update `docs/ENV_SETUP.md` and add `docs/WEB_APP_RUNBOOK.md` whenever any
  env name, port, or run command changes.

## 4. Risk Register

| Risk | Mitigation |
| --- | --- |
| Groq rate limits during demo | Cache `(question -> sql, result)` in `app.query_runs` for 5 min; Retry uses cached SQL when possible. Summarize is skippable to halve the call count. |
| LLM emits SQL referencing non-existent columns | The existing `guard_sql_node` + the new sqlglot identifier check + a "Try fixing" loop that re-prompts with the Trino error message (optional, gated by `APP_AGENT_SQL_REPAIR=true`). |
| Long-running SQL exhausts Trino | Session-level `query_max_execution_time=30s`, backend `asyncio.wait_for`, and Stop button that calls Trino DELETE query. |
| Stop button does not actually cancel | The cancellation registry stores the real Trino query id and hits `DELETE /v1/query/{id}`; tests confirm the query disappears from `SELECT * FROM system.runtime.queries`. |
| Auth misconfiguration leaks data | All routes except `/auth/*`, `/healthz`, `/openapi.json` require `current_user`. Admin-only routes get a negative test. No raw secrets in `/settings/system`. |
| Airflow REST API auth differs across deployments | `airflow_client.py` supports basic auth and JWT, configured via `APP_AIRFLOW_AUTH=basic|jwt`. |
| Layer row counts are expensive | Counts are read from cached `app.layer_stats`, refreshed every 5 min by the scheduler, not on each pageview. The Pipelines card shows the snapshot timestamp so users know it's not live. |
| Topbar status pills hammer services | `/ops/health` is cached 15s server-side and polled 30s client-side; total external probe rate stays under 1 req/svc/min. |
| Summarize text is wrong/hallucinated | Show a "Generated summary" disclaimer on the Answer tab; key_numbers must come from the actual result rows (post-validate that each key_numbers.value is parseable from the result set, otherwise drop it). |
| CSV export leaks rows beyond auth | The `?token=...` is a short-lived (5 min) signed download token bound to `user_id` and `run_id`; not the access token. |

## 5. Definition of Done

A reasonable person can:

1. `docker network create data_network` (once),
2. `cp envs/app.env.example envs/app.env` and fill it,
3. `make postgre-up trino-up minio-up airflow-up app-up`,
4. Open `http://localhost:3000`, log in as the seeded admin,
5. Land on **Ask** with QuickStats populated and 4 sample chips visible,
6. Type a question in Vietnamese and watch the 5-step `AgentStepper` complete,
7. See the four tabs filled: Answer (LLM summary), Chart (auto bar/line/pie),
   Table (sortable/filterable rows), SQL (guarded, copyable),
8. Hit Stop on a long query and see the Trino query disappear,
9. Hit Save and see the run starred in **History** sidebar shortlist,
10. Re-open the saved run in History and see all 4 tabs hydrated without
    re-execution,
11. Open **Catalog**, search "doanh thu", click into `daily_event_summary`
    and copy the `total_revenue` column name,
12. Open **Pipelines** as admin, trigger `bronze_pipeline`, watch the card
    status flip and stream logs,
13. Open **Settings**, switch theme to Dark, change default chart to Bar,
    reload the page and the preferences persist.

All of the above with zero secrets in the repo, all CI checks green
(`python -m compileall code`, `python -m compileall app/backend`,
`npm run lint && npm run test && npm run build`), and the existing CLI agent
(`python code/agent/test_main.py`) still working unchanged.

## 6. Frontend Specification (locked from the proposal)

### 6.1 Information architecture

```text
+---------- Topbar -------------------------------------+
| Agent4DA |  [Trino*] [Spark*] [Airflow*]  [Theme] [u] |
+---------+----------------------------------------------+
| Ask     |                                              |
| History |             Main content (per route)         |
| Catalog |                                              |
|Pipeline |                                              |
|Settings |                                              |
+---------+----------------------------------------------+
```

Sidebar items (top to bottom): Ask, History, Catalog, Pipelines, Settings.
Active item highlighted with the accent color and a left-edge bar.
Below History, a collapsible "Favorites" section shows the latest 5 starred
runs.

### 6.2 Design tokens

Locked to the proposal. Implemented as CSS variables on `:root` (light)
and `[data-theme="dark"]` (dark). All shadcn components must consume these,
not hard-coded hex values.

| Token | Light | Dark |
| --- | --- | --- |
| `--background` | `#F7F8FA` | `#101418` |
| `--surface` | `#FFFFFF` | `#171C22` |
| `--elevated` | `#FFFFFF` | `#1F2630` |
| `--border` | `#E3E7EE` | `#303946` |
| `--text-primary` | `#18202A` | `#E8EEF5` |
| `--text-secondary` | `#647184` | `#A7B2C2` |
| `--accent` | `#2563EB` | `#60A5FA` |
| `--accent-2` | `#0F766E` | `#2DD4BF` |
| `--success` | `#16A34A` | `#4ADE80` |
| `--warning` | `#D97706` | `#FBBF24` |
| `--error` | `#DC2626` | `#F87171` |

Typography: Inter via `next/font` with `Roboto`, `SF Pro`, `system-ui` as
fallbacks. Base size 14px, headings 16/18/20/24px, monospace `JetBrains Mono`
for SQL only.

Radius: 8px on cards, buttons, inputs, badges. Shadow: `0 1px 2px rgba(0,0,0,0.05)`
in light, none in dark (use border + elevated surface instead).

Chart palette (Recharts) reads from `theme-tokens.ts`:
`[accent, accent-2, warning, success, error, #8B5CF6]`. No neon, no gradient
fills, opacity 0.85 in dark mode so series do not glow.

### 6.3 Screen-by-screen specs

**Ask (`/ask`)**

```text
+----------------------------------------------------------+
| QuickStatsStrip  [Today revenue] [MTD events] [Top brand]|
+----------------------------------------------------------+
| QuestionInput (Textarea, language hint, Cmd+Enter)        |
| SampleChips: doanh thu theo ngay | top brand | ...        |
+----------------------------------------------------------+
| AgentStepper                                              |
| (o) Load metadata  (o) Generate  (o) Validate  (o) Run    |
|                                                  (o) Sum  |
+----------------------------------------------------------+
| ResultTabs:  [Answer] [Chart] [Table] [SQL]               |
|                                                           |
|  -- Answer: summary paragraph + key_numbers cards         |
|  -- Chart: type selector + Recharts canvas                |
|  -- Table: TanStack table, 50/page, search, sort          |
|  -- SQL:   syntax-highlighted, guard badge, copy          |
+----------------------------------------------------------+
| ActionBar: Run  Stop  Retry  Copy SQL  Export CSV  Save   |
+----------------------------------------------------------+
```

States: empty (before first run), loading (skeleton on each tab),
streaming (stepper active, tabs show partial), error (red banner with
Retry), stopped (yellow banner). Mobile: tabs scroll horizontally, stepper
becomes vertical.

**History (`/history`)**

Top: filters bar (date range, status multiselect, favorites toggle, search).
Middle: paginated table with sticky header. Right rail (>= xl): "Favorites"
list. Click row -> `/history/[runId]` shows the same Ask layout in
read-only mode with a single "Re-run" button.

**Catalog (`/catalog`)**

List view: search input + zebra `TableList`. Columns: `table_name` (mono),
`display_name`, `KindBadge` (fact teal / dim blue / summary violet / semantic
gray), `purpose`, copy button. Detail view (`/catalog/[tableName]`): header
card with purpose/grain/use_for/query_notes + `ColumnList` zebra table.

**Pipelines (`/pipelines`)**

4 `PipelineCard` tiles (Bronze, Silver, Gold, Metadata). Each: status dot,
last-run time, duration, schedule, `RowCountTile`, action menu (Open runs,
Open logs, Copy debug command, Trigger if admin). Detail view shows
`RunsTable` + `LogViewer`. Auto-refresh every 15s.

**Settings (`/settings`)**

Sections in this order: Theme, Model, Connection status, Defaults. Theme
is the only section that changes the UI instantly; others save on blur
or via an explicit "Save" button per section. Secrets are never shown.

### 6.4 States required on every list/result surface

- **Empty**: icon + headline + 1-line hint + optional CTA. Examples:
  History empty -> "No queries yet, ask your first question on Ask".
- **Loading**: shadcn `Skeleton` boxes matching the final layout's
  rectangles; no spinners except for in-button progress.
- **Error**: red top banner with `<title>` (cause class) + `<reason>` +
  Retry button. Never expose raw stack traces or Trino node URLs.
- **Stopped/Cancelled**: yellow banner with Re-run shortcut.

### 6.5 Responsive

Three breakpoints: mobile (< 768), tablet (768-1023), desktop (>=1024).

- Mobile: sidebar collapses to a hamburger drawer; topbar shrinks (only
  current screen name + status dots, no labels). ResultTabs become a
  bottom-fixed segmented control. ActionBar collapses to a "..." menu.
- Tablet: sidebar shows icons only; tooltips on hover.
- Desktop: full layout.

### 6.6 Accessibility and i18n

- Color contrast >= WCAG AA on both themes (verified for the locked tokens).
- All actions reachable via keyboard; `aria-current` on sidebar active item.
- Focus rings visible (`outline: 2px solid var(--accent)`).
- All UI strings go through a small `t(key)` helper backed by JSON files in
  `frontend/src/i18n/{vi,en}.json`. Default language follows
  `user_preferences.preferred_language`.

### 6.7 Frontend-to-backend endpoint map (final)

| Surface | Endpoint(s) |
| --- | --- |
| Login | `POST /auth/login`, `POST /auth/refresh`, `POST /auth/logout`, `GET /auth/me` |
| Ask: sample chips | `GET /agent/sample-questions` |
| Ask: quick stats | `GET /quickstats` |
| Ask: run | `GET /agent/stream?question=...` (SSE) or `POST /agent/ask` |
| Ask: stop | `POST /agent/stop` `{run_id}` |
| Ask: export CSV | `GET /agent/runs/{run_id}/export.csv?token=...` |
| Ask: save (favorite) | `POST /history/{run_id}/favorite` |
| History | `GET /history`, `GET /history/{run_id}`, `POST/DELETE /history/{run_id}/favorite` |
| Catalog | `GET /catalog/tables`, `GET /catalog/tables/{name}`, `GET /catalog/columns`, `GET /catalog/search` |
| Pipelines | `GET /pipelines`, `GET /pipelines/{dag}/runs`, `GET /pipelines/{dag}/runs/{run}/logs`, `POST /pipelines/{dag}/trigger` (admin), `GET /pipelines/debug-command` |
| Settings | `GET/PUT /settings/me`, `GET /settings/system` |
| Topbar status | `GET /ops/health` (poll 30s) |
| Health probe | `GET /healthz`, `/metrics` |

Every frontend page is one TanStack Query (or SSE) consumer per box; no
component does its own `fetch`. The fetch wrapper in `lib/api.ts` handles
JWT attach, refresh on 401, abort signals (used by Stop), and toasts on
network errors.
