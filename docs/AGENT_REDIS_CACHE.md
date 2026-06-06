# Redis Cache & Rate Limiting

Adds a Redis layer to the backend for speed (caching) and protection (rate
limiting). **Redis is optional**: if it is not running, the app behaves exactly
as before — caches simply miss and the rate limiter fails open. Postgres remains
the source of truth; Redis only ever holds disposable, short-lived copies.

## What it does

| Layer | What's cached / limited | Key | TTL (default) | Invalidation |
|-------|-------------------------|-----|---------------|--------------|
| **Answer cache** | Full `/agent/ask` result | user + session + question + chart + model + conversation-context fingerprint | `APP_CACHE_ANSWER_TTL` = 120s | TTL only; only successful, ≤ `APP_CACHE_ANSWER_MAX_ROWS` rows are stored |
| **Schema/metadata cache** | The Gold schema context the graph builds from Trino | global `a4da:schema_ctx:v1` | `APP_CACHE_SCHEMA_TTL` = 600s | TTL only |
| **Session memory cache** | The follow-up context (rebuilt from `app.query_runs`) | `a4da:sessctx:v1:{session_id}` | `APP_CACHE_SESSION_TTL` = 900s | busted whenever a new turn is saved |
| **Rate limit – ask** | `/agent/ask` + `/agent/stream` | per user | window `APP_RL_ASK_WINDOW_S` = 60s, limit `APP_RL_ASK_LIMIT` = 30 | fixed window |
| **Rate limit – login** | `/auth/login` | per IP + email | window `APP_RL_LOGIN_WINDOW_S` = 60s, limit `APP_RL_LOGIN_LIMIT` = 10 | fixed window |

The answer-cache key includes a fingerprint of the conversation context, so a
follow-up like *"what about February?"* can never be served from a different
session or after the context has moved on. A cache hit still records a new run in
history (so the sidebar stays correct) and is flagged `"cached": true` in the
response; it just skips the LLM + Trino work.

## Design choices

* **Graceful by default.** `api/cache.py` wraps a lazy redis-py client. Any
  failure (library missing, `APP_REDIS_URL` unset, server down, runtime error)
  turns every operation into a safe no-op, and after a failure it stops trying
  for 30s so a dead Redis never adds latency. Rate limiting **fails open** — a
  Redis outage must never lock users out.
* **All Redis code lives in the backend** (`app/backend/api/`). The SQL graph in
  `code/agent` was left untouched: the backend seeds the cached `schema_context`
  into the graph's initial state, and `load_metadata_node` already short-circuits
  when that's present. No new dependency was added to `code/agent`.
* **Session memory is sourced from `app.query_runs`**, the table the live path
  already writes every turn to — so Redis is a pure accelerator over the existing
  database, not a second source of truth.

## Files

* `app/backend/api/cache.py` — Redis client + helpers (get/set/delete JSON, key
  fingerprinting, fail-open rate limiter, health).
* `app/backend/api/settings.py` — `APP_REDIS_URL`, TTLs, rate-limit config.
* `app/backend/api/agent.py` — answer/schema/session caches + ask rate limit.
* `app/backend/api/auth.py` — login rate limit.
* `app/backend/api/main.py` — `/readyz` now reports Redis status.
* `app/backend/requirements.txt` — adds `redis>=5,<6`.
* `docker-compose.redis.yml` — Redis 7 service (`redis-cache`) on `data_network`,
  256 MB cap, LRU eviction, AOF persistence, healthcheck.
* `envs/app.env` — Redis + cache + rate-limit variables.
* `Makefile` — `make redis-up` / `-down` / `-logs`, and included in `make all-up`.

## Run it

```bash
# 1. start Redis (creates data_network if needed)
make redis-up

# 2. install the new dependency and (re)start the backend
#    docker: rebuild the backend image so redis-py is installed
make agent-build && make agent-up
#    local:  pip install -r app/backend/requirements.txt && uvicorn api.main:app --port 8083

# 3. confirm the backend sees Redis
curl -s localhost:8083/readyz
# -> {"status":"ready","cache":{"enabled":true,"connected":true}}
```

Local (non-docker) runs should set `APP_REDIS_URL=redis://localhost:6379/0`
(the in-container default is `redis://redis-cache:6379/0`).

## Tests

```bash
cd app/backend
python tests/test_redis_cache.py          # graceful + functional, no server needed
python tests/test_session_memory_unit.py  # context builder (unchanged behaviour)
```

To see caching live: ask the same question twice in one session — the second
response returns almost instantly with `"cached": true`. Re-ask after
`APP_CACHE_ANSWER_TTL` seconds and it recomputes.

## Tuning / disabling

* Turn everything off without removing code: `APP_CACHE_ENABLED=false` and
  `APP_RATE_LIMIT_ENABLED=false`.
* Longer/shorter memory: adjust the `APP_CACHE_*_TTL` values.
* Stricter abuse protection: lower `APP_RL_*_LIMIT` or shorten the windows.

## Caveats

* Cached answers can be up to `APP_CACHE_ANSWER_TTL` seconds stale if the
  underlying Gold data changes within that window. Keep the TTL short (default
  120s) for fresher data, or set it to a few seconds to effectively disable
  answer caching while keeping schema/session caches.
* Rate-limit windows are fixed (not sliding), which is simple and adequate here;
  switch to a sliding-window/token-bucket scheme if you need smoother limits.
