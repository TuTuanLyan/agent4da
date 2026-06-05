"""Manifest-backed ETL partition state for Bronze/Silver/Gold.

The interface is intentionally small so it can be swapped for PostgreSQL later
without changing Spark/Airflow orchestration code.
"""

import json
from datetime import date, datetime, timezone

from pyspark.sql.types import StringType


PENDING = "PENDING"
DONE = "DONE"
RUNNING = "RUNNING"


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def format_partition_date(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)[:10]


def path_exists(spark, path):
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    jvm = spark.sparkContext._jvm
    uri = jvm.java.net.URI(path)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
    return fs.exists(jvm.org.apache.hadoop.fs.Path(path))


def read_state(spark, state_path):
    if not path_exists(spark, state_path):
        return {"version": 1, "partitions": {}}

    try:
        lines = [row[0] for row in spark.read.text(state_path).collect()]
        payload = json.loads("".join(lines))
    except Exception as exc:
        raise RuntimeError(f"Cannot read ETL partition state at {state_path}") from exc

    if not isinstance(payload, dict):
        raise RuntimeError(f"Invalid ETL partition state at {state_path}: root is not an object.")
    payload.setdefault("version", 1)
    payload.setdefault("partitions", {})
    if not isinstance(payload["partitions"], dict):
        raise RuntimeError(f"Invalid ETL partition state at {state_path}: partitions is not an object.")
    return payload


def write_state(spark, state_path, state):
    state["updated_at"] = utc_now_iso()
    payload = json.dumps(state, sort_keys=True)
    spark.createDataFrame([payload], StringType()).write.mode("overwrite").text(state_path)


def _entry_for(state, partition_date):
    partitions = state.setdefault("partitions", {})
    return partitions.setdefault(
        partition_date,
        {
            "partition_date": partition_date,
            "bronze_status": PENDING,
            "silver_status": PENDING,
            "gold_status": PENDING,
            "affected_by_batches": [],
            "last_error": None,
            "created_at": utc_now_iso(),
        },
    )


def _merge_batches(existing_batches, new_batches):
    merged = {str(batch) for batch in existing_batches or [] if batch not in (None, "")}
    merged.update(str(batch) for batch in new_batches or [] if batch not in (None, ""))
    return sorted(merged)


def mark_bronze_done(spark, state_path, date_rows, affected_batches):
    """Mark Bronze-completed dates and reopen Silver/Gold downstream work."""
    state = read_state(spark, state_path)
    now = utc_now_iso()
    changed_dates = []

    for raw_date, row_count in sorted(date_rows.items()):
        partition_date = format_partition_date(raw_date)
        if partition_date is None:
            continue

        entry = _entry_for(state, partition_date)
        entry["bronze_status"] = DONE
        entry["silver_status"] = PENDING
        entry["gold_status"] = PENDING
        entry["bronze_row_count"] = int(row_count)
        entry["bronze_done_at"] = now
        entry["updated_at"] = now
        entry["last_error"] = None
        entry["affected_by_batches"] = _merge_batches(
            entry.get("affected_by_batches"),
            affected_batches,
        )
        changed_dates.append(partition_date)

    if changed_dates:
        write_state(spark, state_path, state)
    return changed_dates


def pending_silver_dates(spark, state_path, max_dates):
    state = read_state(spark, state_path)
    dates = [
        partition_date
        for partition_date, entry in state.get("partitions", {}).items()
        if entry.get("bronze_status") == DONE
        and entry.get("silver_status", PENDING) == PENDING
    ]
    dates = sorted(dates)
    if max_dates is None or int(max_dates) <= 0:
        return dates
    return dates[: int(max_dates)]


def mark_silver_done(spark, state_path, partition_dates, valid_counts, invalid_counts):
    state = read_state(spark, state_path)
    now = utc_now_iso()
    changed_dates = []

    for raw_date in sorted(partition_dates):
        partition_date = format_partition_date(raw_date)
        if partition_date is None:
            continue
        entry = _entry_for(state, partition_date)
        entry["silver_status"] = DONE
        entry["gold_status"] = PENDING
        entry["silver_valid_row_count"] = int(valid_counts.get(partition_date, 0))
        entry["silver_invalid_row_count"] = int(invalid_counts.get(partition_date, 0))
        entry["silver_done_at"] = now
        entry["updated_at"] = now
        entry["last_error"] = None
        changed_dates.append(partition_date)

    if changed_dates:
        write_state(spark, state_path, state)
    return changed_dates


def mark_silver_pending_with_error(spark, state_path, partition_dates, error):
    state = read_state(spark, state_path)
    now = utc_now_iso()
    changed_dates = []

    for raw_date in sorted(partition_dates):
        partition_date = format_partition_date(raw_date)
        if partition_date is None:
            continue
        entry = _entry_for(state, partition_date)
        entry["silver_status"] = PENDING
        entry["updated_at"] = now
        entry["last_error"] = str(error)[:2000]
        changed_dates.append(partition_date)

    if changed_dates:
        write_state(spark, state_path, state)
    return changed_dates


def _gold_work_candidates(state):
    candidates = []
    for partition_date, entry in state.get("partitions", {}).items():
        if entry.get("silver_status") != DONE:
            continue
        gold_status = entry.get("gold_status", PENDING)
        if gold_status in {PENDING, RUNNING}:
            candidates.append(format_partition_date(partition_date))
    return sorted(date for date in candidates if date is not None)


def _limit_dates(dates, max_dates):
    if max_dates is None or int(max_dates) <= 0:
        return dates
    return dates[: int(max_dates)]


def pending_gold_dates(spark, state_path, max_dates):
    state = read_state(spark, state_path)
    dates = [
        partition_date
        for partition_date, entry in state.get("partitions", {}).items()
        if entry.get("silver_status") == DONE
        and entry.get("gold_status", PENDING) == PENDING
    ]
    return _limit_dates(sorted(dates), max_dates)


def begin_gold_run(spark, state_path, max_dates, run_id=None):
    """Claim the next Gold date set and persist it for downstream Gold tasks.

    Dates left in RUNNING by a killed Spark/Airflow run are treated as retryable.
    Airflow keeps max_active_runs=1, so a new run can safely reclaim them.
    """
    state = read_state(spark, state_path)
    dates = _limit_dates(_gold_work_candidates(state), max_dates)
    now = utc_now_iso()

    state["active_gold_run"] = {
        "run_id": run_id,
        "partition_dates": dates,
        "started_at": now,
        "updated_at": now,
    }

    for partition_date in dates:
        entry = _entry_for(state, partition_date)
        entry["gold_status"] = RUNNING
        entry["gold_run_id"] = run_id
        entry["gold_started_at"] = now
        entry["updated_at"] = now
        entry["last_error"] = None

    write_state(spark, state_path, state)
    return dates


def active_gold_dates(spark, state_path):
    state = read_state(spark, state_path)
    active_run = state.get("active_gold_run") or {}
    active_dates = [
        format_partition_date(value)
        for value in active_run.get("partition_dates", [])
    ]
    active_dates = sorted(date for date in active_dates if date is not None)
    if active_dates:
        return active_dates

    return sorted(
        partition_date
        for partition_date, entry in state.get("partitions", {}).items()
        if entry.get("gold_status") == RUNNING
    )


def _clear_active_gold_run(state, partition_dates):
    active_run = state.get("active_gold_run")
    if not active_run:
        return

    active_dates = {
        format_partition_date(value)
        for value in active_run.get("partition_dates", [])
    }
    target_dates = {format_partition_date(value) for value in partition_dates}
    if active_dates and active_dates.issubset(target_dates):
        state.pop("active_gold_run", None)


def mark_gold_done(spark, state_path, partition_dates, row_counts=None):
    state = read_state(spark, state_path)
    now = utc_now_iso()
    changed_dates = []
    blocked_dates = []
    row_counts = row_counts or {}

    for raw_date in sorted(partition_dates):
        partition_date = format_partition_date(raw_date)
        if partition_date is None:
            continue
        entry = _entry_for(state, partition_date)
        if entry.get("silver_status") != DONE:
            entry["gold_status"] = PENDING
            entry["updated_at"] = now
            entry["last_error"] = (
                "Gold completion skipped because silver_status is no longer DONE."
            )
            entry.pop("gold_run_id", None)
            changed_dates.append(partition_date)
            blocked_dates.append(partition_date)
            continue

        entry["gold_status"] = DONE
        entry["gold_done_at"] = now
        entry["updated_at"] = now
        entry["last_error"] = None
        if partition_date in row_counts:
            entry["gold_row_counts"] = row_counts[partition_date]
        entry.pop("gold_run_id", None)
        changed_dates.append(partition_date)

    if changed_dates:
        _clear_active_gold_run(state, changed_dates)
        write_state(spark, state_path, state)
    if blocked_dates:
        raise RuntimeError(
            "Cannot mark Gold DONE because Silver reopened dates: "
            + ", ".join(blocked_dates)
        )
    return changed_dates


def mark_gold_pending_with_error(spark, state_path, partition_dates, error):
    state = read_state(spark, state_path)
    now = utc_now_iso()
    changed_dates = []

    for raw_date in sorted(partition_dates):
        partition_date = format_partition_date(raw_date)
        if partition_date is None:
            continue
        entry = _entry_for(state, partition_date)
        entry["gold_status"] = PENDING
        entry["updated_at"] = now
        entry["last_error"] = str(error)[:2000]
        entry.pop("gold_run_id", None)
        changed_dates.append(partition_date)

    if changed_dates:
        write_state(spark, state_path, state)
    return changed_dates
