"""Background snapshot jobs for pipeline cards and system status.

The jobs are deliberately best-effort. Missing Iceberg tables during first
project boot are logged and retried on the next interval; they never prevent
the API from starting.
"""

from __future__ import annotations

import asyncio
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import select

from api.settings import get_settings
from db.base import get_sessionmaker
from db.models import LayerStat
from trino_client import execute_query_to_dicts


log = structlog.get_logger("ops.scheduler")
_SCHEDULER: Optional[AsyncIOScheduler] = None

ROW_COUNT_TABLES: Dict[str, Tuple[str, ...]] = {
    "gold": (
        "iceberg.gold.fact_events",
        "iceberg.gold.fact_sales",
        "iceberg.gold.dim_time",
        "iceberg.gold.dim_product",
        "iceberg.gold.dim_user",
        "iceberg.gold.dim_session",
        "iceberg.gold.daily_event_summary",
        "iceberg.gold.daily_product_summary",
        "iceberg.gold.daily_category_summary",
        "iceberg.gold.daily_brand_summary",
    ),
    "metadata": (
        "iceberg.metadata.semantic_table_catalog",
        "iceberg.metadata.semantic_column_catalog",
    ),
}

PARQUET_ROW_PREFIXES: Dict[str, Tuple[str, str]] = {
    "bronze": ("bronze", "ecommerce_events/"),
    "silver": ("silver", "ecommerce_events/"),
}


def _record(
    *,
    layer: str,
    metric_name: str,
    value_int: Optional[int],
    unit: str,
    detail: Optional[dict] = None,
) -> None:
    SessionLocal = get_sessionmaker()
    with SessionLocal() as session:
        session.add(
            LayerStat(
                layer=layer,
                metric_name=metric_name,
                metric_value_bigint=value_int,
                metric_unit=unit,
                detail=detail,
            )
        )
        session.commit()


def _count_table(table: str) -> Optional[int]:
    try:
        rows = execute_query_to_dicts(f"SELECT COUNT(*) AS row_count FROM {table}")
        if rows:
            return int(rows[0]["row_count"])
    except Exception as exc:
        log.warning("layer_stats.table_count_failed", table=table, error=str(exc))
    return None


def refresh_layer_row_counts() -> None:
    for layer, (bucket, prefix) in PARQUET_ROW_PREFIXES.items():
        counts = _count_parquet_rows(bucket, prefix)
        if counts:
            _record(
                layer=layer,
                metric_name="row_count",
                value_int=sum(counts.values()),
                unit="rows",
                detail={"bucket": bucket, "prefix": prefix, "files": counts},
            )
            log.info("layer_stats.parquet_row_count_refreshed", layer=layer, files=len(counts))

    for layer, tables in ROW_COUNT_TABLES.items():
        counts = {}
        for table in tables:
            count = _count_table(table)
            if count is not None:
                counts[table] = count
        if counts:
            _record(
                layer=layer,
                metric_name="row_count",
                value_int=sum(counts.values()),
                unit="rows",
                detail={"tables": counts},
            )
            log.info("layer_stats.row_count_refreshed", layer=layer, tables=len(counts))


def _minio_client():
    from minio import Minio

    settings = get_settings()
    parsed = urlparse(settings.minio_endpoint)
    endpoint = parsed.netloc or parsed.path
    secure = parsed.scheme == "https"
    if not endpoint or not settings.minio_access_key or not settings.minio_secret_key:
        return None
    return Minio(
        endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=secure,
    )


def _count_parquet_rows(bucket: str, prefix: str) -> Dict[str, int]:
    client = _minio_client()
    if client is None:
        return {}

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except Exception as exc:
        log.warning("layer_stats.pyarrow_unavailable", error=str(exc))
        return {}

    try:
        objects = [
            obj
            for obj in client.list_objects(bucket, prefix=prefix, recursive=True)
            if obj.object_name and obj.object_name.endswith(".parquet")
        ]
    except Exception as exc:
        log.warning("layer_stats.parquet_list_failed", bucket=bucket, prefix=prefix, error=str(exc))
        return {}

    counts: Dict[str, int] = {}
    for obj in objects:
        response = None
        try:
            response = client.get_object(bucket, obj.object_name)
            data = response.read()
            parquet = pq.ParquetFile(pa.BufferReader(data))
            counts[obj.object_name] = int(parquet.metadata.num_rows)
        except Exception as exc:
            log.warning(
                "layer_stats.parquet_count_failed",
                bucket=bucket,
                object_name=obj.object_name,
                error=str(exc),
            )
        finally:
            if response is not None:
                response.close()
                response.release_conn()
    return counts


def _bucket_stats(bucket: str) -> Optional[Tuple[int, int]]:
    client = _minio_client()
    if client is None:
        return None
    try:
        objects = list(client.list_objects(bucket, recursive=True))
    except Exception as exc:
        log.warning("layer_stats.bucket_failed", bucket=bucket, error=str(exc))
        return None
    return len(objects), sum(int(getattr(obj, "size", 0) or 0) for obj in objects)


def refresh_object_stats() -> None:
    settings = get_settings()
    buckets = {
        "bronze": settings.minio_bucket_bronze,
        "silver": settings.minio_bucket_silver,
        "gold": settings.minio_bucket_gold,
    }
    for layer, bucket in buckets.items():
        stats = _bucket_stats(bucket)
        if stats is None:
            continue
        object_count, byte_count = stats
        _record(
            layer=layer,
            metric_name="object_count",
            value_int=object_count,
            unit="objects",
            detail={"bucket": bucket},
        )
        _record(
            layer=layer,
            metric_name="byte_size",
            value_int=byte_count,
            unit="bytes",
            detail={"bucket": bucket},
        )
        log.info("layer_stats.object_stats_refreshed", layer=layer, bucket=bucket)


def refresh_layer_stats_once() -> None:
    refresh_layer_row_counts()
    refresh_object_stats()


async def _refresh_once_async() -> None:
    await asyncio.to_thread(refresh_layer_stats_once)


def start_scheduler() -> Optional[AsyncIOScheduler]:
    global _SCHEDULER
    settings = get_settings()
    if not settings.enable_scheduler:
        log.info("scheduler.disabled")
        return None
    if _SCHEDULER is not None and _SCHEDULER.running:
        return _SCHEDULER

    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(
        refresh_layer_stats_once,
        "interval",
        minutes=5,
        id="refresh_layer_stats",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()
    _SCHEDULER = scheduler
    asyncio.create_task(_refresh_once_async())
    log.info("scheduler.started")
    return scheduler


def stop_scheduler() -> None:
    global _SCHEDULER
    if _SCHEDULER is not None and _SCHEDULER.running:
        _SCHEDULER.shutdown(wait=False)
        log.info("scheduler.stopped")
    _SCHEDULER = None


def latest_row_count(layer: str) -> Optional[int]:
    SessionLocal = get_sessionmaker()
    with SessionLocal() as session:
        stat = session.execute(
            select(LayerStat)
            .where(LayerStat.layer == layer, LayerStat.metric_name == "row_count")
            .order_by(LayerStat.measured_at.desc())
            .limit(1)
        ).scalar_one_or_none()
        return stat.metric_value_bigint if stat is not None else None
