from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends

from .auth import current_user
from .integrations import probe_http
from .settings import get_settings

router = APIRouter(prefix="/ops", tags=["ops"])


def snapshot(status: str, *, version=None, workers=None, latency_ms=None, detail=None) -> dict:
    return {
        "status": status,
        "version": version,
        "workers": workers,
        "latency_ms": latency_ms,
        "detail": detail,
    }


@router.get("/health")
def health(_user: dict = Depends(current_user)) -> dict:
    settings = get_settings()

    trino_status, _, trino_latency, trino_detail = probe_http(
        f"http://{settings.trino_host}:{settings.trino_port}/v1/info"
    )
    spark_status, _, spark_latency, spark_detail = probe_http(
        settings.spark_master_url.rstrip("/") + "/json/"
    )
    airflow_status, _, airflow_latency, airflow_detail = probe_http(
        settings.airflow_base_url.rstrip("/") + "/health"
    )

    return {
        "trino": snapshot(trino_status, latency_ms=trino_latency, detail=trino_detail),
        "spark": snapshot(spark_status, latency_ms=spark_latency, detail=spark_detail),
        "airflow": snapshot(airflow_status, latency_ms=airflow_latency, detail=airflow_detail),
        "gemini": snapshot("configured" if (settings.gemini_api_key or settings.gemini_api_keys) else "missing"),
        "groq": snapshot("configured" if settings.groq_api_key else "missing"),
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }
