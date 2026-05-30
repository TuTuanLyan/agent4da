"""In-memory registry of running agent tasks for Stop / cancellation.

Maps run_id -> {task, trino_query_id, started_at, user_id}.

When POST /agent/stop fires we look up the entry, attempt to cancel the
Trino query via the Trino REST DELETE /v1/query/{id} call, then cancel
the asyncio task. The agent service catches asyncio.CancelledError and
marks the run as `status='stopped'`.

Process-local only; if you scale beyond one backend pod you need a shared
store (Redis). MVP runs on a single replica.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Dict, Optional
from urllib import request

import structlog

from api.settings import get_settings


log = structlog.get_logger("agent.cancellation")


@dataclass
class RunHandle:
    run_id: str
    user_id: str
    task: asyncio.Task
    trino_query_id: Optional[str] = None
    started_at: float = field(default_factory=time.time)


_REGISTRY: Dict[str, RunHandle] = {}


def register(run_id: str, user_id: str, task: asyncio.Task) -> RunHandle:
    handle = RunHandle(run_id=run_id, user_id=user_id, task=task)
    _REGISTRY[run_id] = handle
    return handle


def set_trino_query_id(run_id: str, trino_query_id: Optional[str]) -> None:
    handle = _REGISTRY.get(run_id)
    if handle is not None:
        handle.trino_query_id = trino_query_id


def get(run_id: str) -> Optional[RunHandle]:
    return _REGISTRY.get(run_id)


def unregister(run_id: str) -> None:
    _REGISTRY.pop(run_id, None)


def cancel_trino_query(handle: RunHandle) -> bool:
    if not handle.trino_query_id:
        return False
    settings = get_settings()
    url = f"http://{settings.trino_host}:{settings.trino_port}/v1/query/{handle.trino_query_id}"
    try:
        req = request.Request(url, method="DELETE")
        with request.urlopen(req, timeout=3):
            pass
        log.info(
            "agent.trino_query_cancelled",
            run_id=handle.run_id,
            trino_query_id=handle.trino_query_id,
        )
        return True
    except Exception as exc:
        log.warning(
            "agent.trino_query_cancel_failed",
            run_id=handle.run_id,
            trino_query_id=handle.trino_query_id,
            error=str(exc),
        )
        return False
