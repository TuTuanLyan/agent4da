# Exporters

This monitoring module uses the following Prometheus exporters. They are all
defined in `../docker-compose.monitoring.yml` and run on the shared
`data_network`. **None of them publish a host port** — Prometheus scrapes them
internally by container name, which keeps the host port footprint to just
Prometheus (19090) and Grafana (13000).

| Exporter | Image | Internal target | What it provides |
|---|---|---|---|
| node-exporter | `prom/node-exporter` | `node-exporter:9100` | Host CPU, memory, disk, filesystem, network |
| cAdvisor | `gcr.io/cadvisor/cadvisor` | `cadvisor:8080` | Per-container CPU / memory / I/O |
| postgres-exporter | `quay.io/prometheuscommunity/postgres-exporter` | `postgres-exporter:9187` | PostgreSQL up, connections, database size |

The FastAPI/AI-Agent backend and Trino are **not** exporters — they expose
Prometheus metrics natively at `agent4da:8000/metrics` and `trino:8080/metrics`
respectively, and are scraped directly.

## postgres-exporter credentials

The connection string defaults (in the compose file) match `envs/postgre.env`
(user `bigdata`, db `agent4da`). The password contains a `#`, which must be
**URL-encoded** in the DSN (`#` → `%23`), so the default is `%233Bigdata`.

Override without editing the compose file by exporting host env vars before
`up`:

```bash
export MON_POSTGRES_USER=bigdata
export MON_POSTGRES_PASSWORD=%233Bigdata   # url-encoded
export MON_POSTGRES_DB=agent4da
```

## cAdvisor note

cAdvisor needs privileged access and host mounts. On some hosts (cgroup v2 or
restricted environments) it may fail to start. If so, comment out the `cadvisor`
service in the compose file and remove the `cadvisor` scrape job in
`../prometheus/prometheus.yml`; host-level resource metrics from node-exporter
are unaffected.
