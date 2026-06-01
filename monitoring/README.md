# Agent4DA — Monitoring & Observability Module

A **self-contained** Prometheus + Grafana stack for observing the Agent4DA
platform: hardware/container resources, service health, ETL pipeline status, AI
Agent response speed, and query performance.

> **This is a separate module from the App UI.**
> Prometheus and Grafana are **not** part of the user-facing frontend. They have
> their own standalone web UIs and are intended for **system administrators,
> developers, and operators only** — not normal end users.
>
> Nothing here embeds dashboards or metrics pages into the App UI. The end-user
> frontend remains only for chatting with the AI Agent and viewing results.

Everything for this module lives under `monitoring/` (plus a small, clearly
listed set of metrics hooks in the backend — see
[Files changed outside `monitoring/`](#files-changed-outside-monitoring)).

---

## Access URLs

| UI | URL | Notes |
|---|---|---|
| **Prometheus** | http://localhost:19090 | Targets, queries, alerts |
| **Grafana** | http://localhost:13000 | Dashboards. Default login `admin` / `admin` |

### Why not the default ports 9090 / 3000?

Host ports were chosen to avoid conflicts with the existing stack:

* **Grafana 3000 is already taken** — the frontend (`agent4da-ui`) publishes
  `3000:3000`. So Grafana is mapped to **host `13000`** (container stays 3000).
* Prometheus is mapped to **host `19090`** (container stays 9090) per project
  convention, to steer clear of the commonly-occupied 9090.

Existing host ports in the project: 8083 (backend), 3000 (frontend), 8081/8793
(airflow), 5432 (postgres), 9092 (kafka), 9000/9001 (minio), 8080/7077/4040
(spark), 8082 (trino). `19090` and `13000` are both free. The exporters
(node-exporter, cAdvisor, postgres-exporter) are **not** published on the host —
Prometheus scrapes them internally — so the module adds only two host ports.

If `19090` or `13000` are also busy on your host, change the left-hand side of
the `ports:` mapping in `docker-compose.monitoring.yml` and update the URLs
above.

---

## How to run

The monitoring stack attaches to the project's existing external Docker network
`data_network`, so **start the application stack first** (which creates the
network), then start monitoring.

```bash
# 1. Ensure the shared network exists (the project Makefile does this too)
docker network inspect data_network >/dev/null 2>&1 || docker network create data_network

# 2. Start the app stack as usual (example)
make all-up        # or your usual per-service `make *-up` commands

# 3. Start the monitoring module (from the repo root)
docker compose -f monitoring/docker-compose.monitoring.yml up -d

#    ...or from inside the monitoring folder:
#    cd monitoring && docker compose -f docker-compose.monitoring.yml up -d
```

Stop it with:

```bash
docker compose -f monitoring/docker-compose.monitoring.yml down
```

> The backend image must be rebuilt once so the new `/metrics` endpoint and the
> `prometheus-client` dependency are present:
> `make agent-build && make agent-up`.

---

## What gets collected

| Source | How | Job in Prometheus |
|---|---|---|
| Host CPU / memory / disk / network | node-exporter | `node-exporter` |
| Per-container resource usage | cAdvisor | `cadvisor` |
| PostgreSQL up / connections / db size | postgres-exporter | `postgres-exporter` |
| FastAPI HTTP + AI Agent + ETL metrics | backend `/metrics` | `agent-backend` |
| Trino engine metrics | Trino native `/metrics` (OpenMetrics) | `trino` |
| Prometheus itself | self-scrape | `prometheus` |

### Collected directly (no code change)
Host resources, container resources, PostgreSQL, Trino engine metrics, and
Prometheus self-metrics — all via off-the-shelf exporters / native endpoints.

### Required backend instrumentation
HTTP request counts/latency, AI Agent `/ask` (requests, success, errors by type,
total response time, retries), agent-side **Trino query** count/duration/slow
count, **SQL generation** duration, and **ETL pipeline** status. These are
exposed at `agent4da:8000/metrics` (see the files list below).

### ETL pipeline metrics — how
Airflow has no native Prometheus endpoint here, and adding a statsd-exporter
stack would be heavier than warranted. Instead the backend already talks to the
Airflow REST API (`pipelines.py`); a lightweight background thread mirrors DAG
run state (`bronze/silver/gold/gold_metadata`) into Prometheus gauges:

* `agent4da_etl_pipeline_last_run_status{dag_id,layer}` — `1`=success, `0`=failed, `-1`=unknown/running/none
* `agent4da_etl_pipeline_last_duration_seconds{dag_id,layer}`
* `agent4da_etl_pipeline_last_success_timestamp_seconds{dag_id,layer}` (freshness)
* `agent4da_etl_pipeline_paused{dag_id,layer}`
* `agent4da_etl_collector_up` — `1` if Airflow is reachable

---

## Dashboards (Grafana → folder "Agent4DA")

| Dashboard | File | Panels |
|---|---|---|
| **System Overview** | `grafana/dashboards/system-overview.json` | Service up/down, host CPU/memory/disk, container CPU/memory |
| **AI Agent Performance** | `grafana/dashboards/ai-agent-performance.json` | Total /ask, success & error rate, errors by type, avg/p50/p95/p99 response time, SQL-gen duration, Trino duration, retries |
| **ETL Pipeline Monitoring** | `grafana/dashboards/etl-pipeline-monitoring.json` | Bronze/Silver/Gold status, last duration, last successful run, Gold freshness, collector reachability, failed count |
| **Query / Data Layer** | `grafana/dashboards/query-data-layer.json` | Trino query count/rate/latency, slow & failed queries, PostgreSQL status, connections, database size |

Dashboards and the Prometheus datasource are **auto-provisioned** on Grafana
startup, so they appear without manual import.

---

## Folder layout

```
monitoring/
├── docker-compose.monitoring.yml      # Prometheus, Grafana, node-exporter, cAdvisor, postgres-exporter
├── prometheus/
│   ├── prometheus.yml                 # scrape config (this is where targets live)
│   └── alert_rules.yml                # alerting rules
├── grafana/
│   ├── provisioning/
│   │   ├── datasources/datasource.yml # Prometheus datasource (uid: prometheus)
│   │   └── dashboards/dashboards.yml  # dashboard provider
│   └── dashboards/                    # the 4 dashboard JSONs
├── exporters/
│   └── README.md                      # exporter details & postgres-exporter creds
└── README.md                          # this file
```

---

## Verifying it works

1. **Backend metrics endpoint** (inside the network or from the host via the
   backend's published port `8083`):
   ```bash
   curl -s http://localhost:8083/metrics | grep agent4da_ | head
   ```
   You should see `agent4da_http_requests_total`, `agent4da_ask_requests_total`,
   `agent4da_etl_pipeline_last_run_status`, etc. (counters start at 0 / appear
   after the first `/agent/ask`).

2. **Prometheus targets** — open http://localhost:19090/targets and confirm
   `agent-backend`, `node-exporter`, `cadvisor`, `postgres-exporter`, `trino`,
   and `prometheus` are **UP**.

3. **Prometheus alerts** — http://localhost:19090/alerts lists the rules.

4. **Grafana** — http://localhost:13000 (admin/admin), open the **Agent4DA**
   folder, confirm the 4 dashboards render and the Prometheus datasource tests OK.

5. **Generate agent traffic** — make a few `/agent/ask` calls, then watch the
   AI Agent Performance dashboard populate.

---

## Files changed outside `monitoring/`

The monitoring stack is isolated, but exposing metrics requires a few minimal,
clearly-scoped hooks. **No App UI / frontend file was touched.**

| File | Change | Why |
|---|---|---|
| `app/backend/requirements.txt` | add `prometheus-client` | dependency for the metrics endpoint |
| `app/backend/api/obs_metrics.py` | **new** | metric definitions, `GET /metrics`, HTTP/`/ask` helpers, ETL refresher |
| `app/backend/api/main.py` | import + 1 middleware hook + include router + start refresher | record HTTP metrics, expose `/metrics`, start ETL mirror |
| `app/backend/api/agent.py` | thin timing wrapper around `execute_question` | record `/ask` count/success/errors/latency/retries |
| `code/agent/services/obs_metrics.py` | **new** (guarded) | agent-side Trino + LLM/SQL-gen metrics |
| `code/agent/services/trino_service.py` | wrap query exec in a timing context | Trino query duration / success / slow count |
| `code/agent/services/llm_service.py` | wrap LLM calls in a timing context | SQL-generation duration |

All instrumentation is defensive: if `prometheus_client` is missing or a metric
op fails, it degrades to a no-op and never changes agent behaviour. No DAG,
Spark job, Trino query logic, or Gold-layer generation logic was modified.

---

## Alerting

`prometheus/alert_rules.yml` ships ready-to-evaluate rules (visible at
`/alerts`): backend down, Postgres down, Trino/node-exporter down, high `/ask`
error rate, high `/ask` p95 latency, high Trino query p95 latency, ETL pipeline
failed, and Gold-layer-stale (no successful gold run in 24h), plus host
CPU/memory/disk pressure.

**No notification channel is wired by default** (kept simple for the demo). To
send notifications, run an Alertmanager and uncomment the `alerting:` block in
`prometheus/prometheus.yml` — the rules need no changes.

---

## Limitations & follow-ups

* **Single backend process assumed.** Agent metrics are accumulated in-process
  (the agent runs in the FastAPI process). If you scale uvicorn to multiple
  workers, enable `prometheus_client` multiprocess mode, otherwise each worker
  reports its own counters.
* **Trino native `/metrics`** always requires a *username* on the request, even
  when Trino has no authentication configured — otherwise Trino returns HTTP 401
  and Prometheus shows the target as DOWN (while Trino itself is perfectly
  alive). An unsecured Trino does **not** honor HTTP Basic auth, but it does
  read the `X-Trino-User` header, so the `trino` job sends
  `http_headers: X-Trino-User: agent4da` (any username works, no password). This
  needs Prometheus ≥ 2.49 (the stack pins v2.53). If you enable real Trino auth,
  switch to `basic_auth` with a system-information user and a `password_file`.
* **Airflow** has no native Prometheus endpoint here; ETL metrics come from the
  Airflow REST API via the backend collector. For richer native Airflow metrics
  you would add a statsd-exporter (out of scope for this module).
* **cAdvisor** may fail on some hosts (cgroup v2 / permissions). It can be
  disabled without affecting host-level metrics — see `exporters/README.md`.
* **postgres-exporter password** default is URL-encoded in the compose file;
  override via `MON_POSTGRES_*` env vars (see `exporters/README.md`).
