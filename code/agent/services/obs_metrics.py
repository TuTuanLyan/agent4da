"""
Lightweight, dependency-tolerant Prometheus instrumentation for the AI Agent
services (Trino execution + LLM SQL generation).

This module belongs to the SEPARATE monitoring/observability concern. It only
*observes* the agent; it never changes agent behaviour. If ``prometheus_client``
is not installed (e.g. the agent code is imported in a context without it),
every helper degrades to a no-op so the agent keeps working unchanged.

The agent runs in-process inside the FastAPI backend, so the metrics defined
here register in the same default Prometheus registry that the backend exposes
at ``GET /metrics`` — no extra endpoint is needed on the agent side.
"""
from __future__ import annotations

import os
import time
from contextlib import contextmanager

try:  # prometheus_client is optional; agent must not break without it.
    from prometheus_client import REGISTRY, Counter, Histogram

    _ENABLED = True
except Exception:  # pragma: no cover
    _ENABLED = False

# Queries slower than this many seconds are counted as "slow".
SLOW_QUERY_SECONDS = float(os.getenv("TRINO_SLOW_QUERY_SECONDS", "5"))

_DURATION_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 60)


class _Noop:
    """Stand-in used when prometheus_client is unavailable or on conflicts."""

    def labels(self, *args, **kwargs):
        return self

    def inc(self, *args, **kwargs):
        pass

    def observe(self, *args, **kwargs):
        pass


def _find(sample_name):
    try:
        for collector, names in list(REGISTRY._collector_to_names.items()):
            if sample_name in names:
                return collector
    except Exception:  # pragma: no cover
        pass
    return None


def _counter(name, doc, labels=()):
    if not _ENABLED:
        return _Noop()
    try:
        return Counter(name, doc, list(labels))
    except ValueError:  # already registered (re-import under another name)
        return _find(name + "_total") or _Noop()


def _hist(name, doc, labels=()):
    if not _ENABLED:
        return _Noop()
    try:
        return Histogram(name, doc, list(labels), buckets=_DURATION_BUCKETS)
    except ValueError:
        return _find(name + "_bucket") or _Noop()


# --- Metric objects ----------------------------------------------------------
TRINO_QUERIES = _counter(
    "agent4da_trino_queries", "Trino queries executed by the agent", ["status"]
)
TRINO_DURATION = _hist(
    "agent4da_trino_query_duration_seconds", "Trino query execution time (agent side)"
)
TRINO_SLOW = _counter(
    "agent4da_trino_slow_queries",
    f"Trino queries slower than {SLOW_QUERY_SECONDS:g}s",
)
SQLGEN_DURATION = _hist(
    "agent4da_sql_generation_duration_seconds", "LLM SQL-generation duration"
)
LLM_REQUESTS = _counter("agent4da_llm_requests", "LLM calls", ["kind", "status"])
LLM_DURATION = _hist("agent4da_llm_request_duration_seconds", "LLM call duration", ["kind"])


# --- Context managers --------------------------------------------------------
@contextmanager
def observe_trino_query():
    """Time a Trino query, recording duration, success/error and slow count."""
    start = time.perf_counter()
    ok = True
    try:
        yield
    except Exception:
        ok = False
        raise
    finally:
        duration = time.perf_counter() - start
        try:
            TRINO_DURATION.observe(duration)
            TRINO_QUERIES.labels(status="success" if ok else "error").inc()
            if ok and duration > SLOW_QUERY_SECONDS:
                TRINO_SLOW.inc()
        except Exception:  # pragma: no cover
            pass


@contextmanager
def observe_llm(kind):
    """Time an LLM call. ``kind`` is e.g. 'sql' or 'text'."""
    start = time.perf_counter()
    ok = True
    try:
        yield
    except Exception:
        ok = False
        raise
    finally:
        duration = time.perf_counter() - start
        try:
            LLM_DURATION.labels(kind=kind).observe(duration)
            LLM_REQUESTS.labels(kind=kind, status="success" if ok else "error").inc()
            if kind == "sql":
                SQLGEN_DURATION.observe(duration)
        except Exception:  # pragma: no cover
            pass
