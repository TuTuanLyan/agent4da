# Data ETL Batch Processing

This document records the ETL state and the move from file/full-refresh oriented
processing to event-date incremental processing. The current implementation adds
producer metadata, Bronze `event_date` partitioning, manifest-backed
Bronze/Silver/Gold state tracking, Silver pending-date processing, and Gold
incremental processing for bounded pending `event_date` batches. The Agent layer
is out of scope and was not changed.

## Scope And Non-Goals

- Scope: Kafka producer, Bronze, Silver, Gold, Gold metadata, Airflow orchestration,
  and a minimal ETL control state store.
- Non-goal: no changes to `code/agent/`, LangGraph nodes, Text-to-SQL,
  metadata service, Trino service, or guardrails.
- Official batch key from Silver onward: `event_date`, parsed from `event_time`.
  File name, folder name, and assumed monthly/daily input slices must not drive
  batch selection.
- Producer can still be triggered manually with a CSV file. Optional chunking is
  allowed at producer/Kafka level, but the producer should not understand
  business dates.
- Spark/Airflow must use the JARs already mounted under `/opt/project/jars`
  through classpath (`driver_class_path`, `spark.executor.extraClassPath`).
  Do not use `--packages`/Ivy dependency downloads in DAG runs.

## Files Read

- Project/docs: `README.md`, `PROJECT.md`, `docs/`, `code/airflow/dags/AIRFLOW_DAGS.md`,
  `code/spark/SPARK.md`, `code/kafka/KAFKA.md`.
- Kafka/Bronze/Silver: `code/kafka/producer.py`, `script/kafka/producer.sh`,
  `code/spark/bronze_job.py`, `code/spark/silver_job.py`,
  `code/spark/common/config.py`, `code/spark/common/s3a.py`.
- Airflow: `code/airflow/dags/bronze_pipeline.py`,
  `code/airflow/dags/silver_pipeline.py`,
  `code/airflow/dags/gold_pipeline.py`,
  `code/airflow/dags/gold_metadata_pipeline.py`,
  `code/airflow/dags/dag_common.py`.
- Gold: `code/spark/gold/config.py`, `code/spark/gold/ddl.py`,
  `code/spark/gold/writers.py`, `code/spark/gold/staging.py`,
  `code/spark/gold/facts.py`, `code/spark/gold/dimensions.py`,
  `code/spark/gold/summaries.py`, `code/spark/gold/metadata.py`,
  `code/spark/gold/metadata_schema.py`,
  `code/spark/gold/metadata_definitions.py`, and all task entrypoints in
  `code/spark/gold/tasks/`.
- Runtime catalog/control context: `init/01_init_schemas.sh`,
  `docs/ENV_SETUP.md`, `docs/TRINO_ENV.md`, `docs/CONVERT_BUCKET.md`.

## Current State

- Kafka producer reads a CSV and sends each row as JSON to `ecommerce_events`.
  It remains manual and date-agnostic, but now adds `source_file`,
  `ingestion_batch_id`, `chunk_id`, and `ingest_time`. It also flushes messages
  every `--chunk-size` rows, default `10000`.
- Producer args `--batch-id` and backward-compatible alias `--batch` only set
  `ingestion_batch_id` metadata. They do not split by date, do not change Kafka
  offsets, and are separate from the ETL batch key `event_date`.
- Bronze reads Kafka with Spark batch, using a MinIO JSON offset file:
  `s3a://bronze/_offsets/ecommerce_events.json`.
- Bronze currently parses JSON as strings, adds Kafka metadata and `ingested_at`,
  parses `event_time` into `event_ts` and `event_date`, keeps `kafka_date`, and
  writes Bronze partitioned by business `event_date`.
- Bronze updates manifest-backed partition state after a successful write and
  before committing Kafka offsets.
- Silver reads pending dates from partition state and reads only matching Bronze
  `event_date=YYYY-MM-DD` partitions by default.
- Silver validates rows, deduplicates by `event_fingerprint`, deletes matching
  Silver output partitions, and writes replacements partitioned by `event_date`.
  Legacy full scan is available only with `SILVER_FULL_SCAN_FALLBACK=true`.
- Gold is active in code and manual in Airflow. The default Gold mode is now
  `incremental`. Each run claims at most `MAX_GOLD_DATES_PER_RUN` dates, default
  `3`, where `silver_status=DONE` and `gold_status` is retryable.
- Gold incremental writes are idempotent:
  - staging replaces `gold_staging.stg_events` partitions by `event_date`;
  - facts replace `fact_events.event_date` and `fact_sales.sale_date`;
  - `dim_time` replaces by `event_date`;
  - product/user/session dimensions recompute affected keys and replace those
    keys;
  - summaries replace daily summary partitions by `event_date`.
- `gold_pipeline` now refreshes and validates semantic metadata after summaries,
  then runs `mark_gold_done`. Gold status becomes `DONE` only after facts,
  dimensions, summaries, metadata refresh, and metadata validation succeed.
- Gold metadata remains small and full-refresh oriented. The standalone
  `gold_metadata_pipeline` is still manual/admin.

## Full Refresh Compatibility

- `full_refresh` remains available for admin rebuilds only:

```bash
airflow dags trigger gold_pipeline --conf '{"mode":"full_refresh"}'
```

- Full refresh uses the existing `write_full_refresh()` path and does not mutate
  per-date manifest statuses.
- Incremental is the normal Gold DAG mode:

```bash
airflow dags trigger gold_pipeline
```

## Current State, Checkpoint, And Status

- Existing checkpoint/state:
  - Bronze Kafka offsets are stored in MinIO JSON.
  - Bronze/Silver partition status is stored in a MinIO manifest JSON at
    `s3a://bronze/_state/etl_partition_status.json` by default.
  - Airflow has DAG/task run state in the Airflow metadata database.
  - Iceberg has table metadata/snapshots in PostgreSQL JDBC catalog.
- Remaining gaps:
  - PostgreSQL ETL control table is not implemented yet.
  - Manifest state is not atomic for high-concurrency writers; keep
    `max_active_runs=1` on Bronze, Silver, and Gold.
  - Product/user/session dimensions recompute keys observed in the current
    incremental result. If a reprocessed date removes every event for an old key,
    those dimension aggregates may need a manual full refresh to remove the old
    contribution.

## Implemented Manifest State

The pipeline currently uses a manifest JSON rather than PostgreSQL because
Bronze/Silver do not yet have a dedicated PostgreSQL/JDBC helper or PostgreSQL
jar in the base Bronze/Silver DAG classpath. The implementation is isolated in
`code/spark/common/partition_state.py` so a later PostgreSQL backend can replace
the manifest without changing the Spark/Airflow orchestration shape.

Default state path:

```text
s3a://bronze/_state/etl_partition_status.json
```

Override:

```text
ETL_PARTITION_STATE_PATH=s3a://<bucket>/_state/etl_partition_status.json
```

Logical manifest shape:

```json
{
  "version": 1,
  "updated_at": "2026-06-05T00:00:00+00:00",
  "active_gold_run": {
    "run_id": "manual__2026-06-05T00:00:00+00:00",
    "partition_dates": ["2020-01-01"],
    "started_at": "2026-06-05T00:00:00+00:00",
    "updated_at": "2026-06-05T00:00:00+00:00"
  },
  "partitions": {
    "2020-01-01": {
      "partition_date": "2020-01-01",
      "bronze_status": "DONE",
      "silver_status": "PENDING",
      "gold_status": "PENDING",
      "bronze_row_count": 1000,
      "silver_valid_row_count": 990,
      "silver_invalid_row_count": 10,
      "affected_by_batches": ["<ingestion_batch_id>"],
      "last_error": null,
      "created_at": "...",
      "updated_at": "...",
      "bronze_done_at": "...",
      "silver_done_at": "...",
      "gold_started_at": "...",
      "gold_done_at": "..."
    }
  }
}
```

Status transitions:

- Bronze success for an affected date sets `bronze_status=DONE`,
  `silver_status=PENDING`, `gold_status=PENDING`.
- Silver selects dates where `bronze_status=DONE` and `silver_status=PENDING`.
- Silver success sets `silver_status=DONE` and keeps `gold_status=PENDING`.
- Silver failure keeps `silver_status=PENDING` and records `last_error`, so the
  date can retry on a later scheduled run.
- Gold prepare claims a bounded set of dates into `active_gold_run`, sets
  `gold_status=RUNNING`, and reads only matching Silver partitions.
- Gold data task or metadata failure resets active dates to `gold_status=PENDING`
  and records `last_error`.
- Gold success marks active dates `gold_status=DONE` only in `mark_gold_done`,
  after data writes and metadata validation complete.

## Mixed-Date File Handling

Input CSV files may contain many days or months in any order. The producer does
not inspect or split by date; it only sends rows to Kafka with source/batch/chunk
metadata. Bronze derives `event_date` from each row's `event_time`. A single
producer batch can therefore update many manifest entries, one per affected
`event_date`.

Silver does not care which file or chunk created a row. It reads the manifest,
chooses pending `event_date` values, and reads the matching Bronze partitions.
This means a mixed-month file naturally becomes multiple date partitions after
Bronze and multiple small Silver replacement batches over one or more Silver
runs.

## Tracking Schema Proposal

PostgreSQL remains the recommended long-term tracking store. The project already has
PostgreSQL, `ICEBERG_JDBC_*` connection settings, and init schemas
`bronze_meta` and `silver_meta`. Add a small ETL control schema rather than
mixing operational status into Iceberg catalog tables.

Minimal table:

```sql
CREATE SCHEMA IF NOT EXISTS etl_control;

CREATE TABLE IF NOT EXISTS etl_control.event_date_status (
  event_date date PRIMARY KEY,
  bronze_status text NOT NULL DEFAULT 'PENDING',
  silver_status text NOT NULL DEFAULT 'PENDING',
  gold_status text NOT NULL DEFAULT 'PENDING',
  bronze_run_id text,
  silver_run_id text,
  gold_run_id text,
  bronze_row_count bigint DEFAULT 0,
  silver_valid_row_count bigint DEFAULT 0,
  silver_invalid_row_count bigint DEFAULT 0,
  gold_fact_row_count bigint DEFAULT 0,
  gold_summary_row_count bigint DEFAULT 0,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  bronze_done_at timestamptz,
  silver_done_at timestamptz,
  gold_done_at timestamptz,
  CHECK (bronze_status IN ('PENDING', 'RUNNING', 'DONE', 'FAILED')),
  CHECK (silver_status IN ('PENDING', 'RUNNING', 'DONE', 'FAILED')),
  CHECK (gold_status IN ('PENDING', 'RUNNING', 'DONE', 'FAILED'))
);

CREATE INDEX IF NOT EXISTS idx_event_date_status_silver_pending
  ON etl_control.event_date_status (event_date)
  WHERE bronze_status = 'DONE' AND silver_status = 'PENDING';

CREATE INDEX IF NOT EXISTS idx_event_date_status_gold_pending
  ON etl_control.event_date_status (event_date)
  WHERE silver_status = 'DONE' AND gold_status = 'PENDING';
```

Optional once Airflow/Spark concurrency grows:

- `etl_control.pipeline_run_dates(run_id, layer, event_date, claimed_at)` to keep
  a stable batch manifest across tasks.
- `attempt_count`, `locked_by`, `locked_at`, and status reset queries for failed
  or abandoned runs.

The current manifest backend is simpler but weaker than PostgreSQL: concurrent
updates, atomic claims, and manual recovery are harder. Keep
`max_active_runs=1` for Bronze, Silver, and Gold while this backend is in use.

## Implemented Flow

1. Manifest state backend.
   - Added manifest helper in `code/spark/common/partition_state.py`.
   - Added config fields for `ETL_PARTITION_STATE_PATH` and
     `MAX_SILVER_DATES_PER_RUN`.
   - Keep Airflow `max_active_runs=1` while manifest state is in use.

2. Producer remains manual and date-agnostic.
   - Keep `producer.py --file ...` flow.
   - Optionally add `--chunk-size`/`--flush-every` to reduce memory and publish
     progress, but do not infer date windows in producer.

3. Bronze parses event time and records affected dates.
   - Implemented parse of `event_time` into `event_ts` and `event_date`.
   - Bronze now writes physical partitions by `event_date`.
   - Existing Kafka offset file is kept and updated only after Bronze write and
     manifest update succeed.
   - Manifest is upserted for each affected non-null `event_date` with
     `bronze_status=DONE`, `silver_status=PENDING`, and `gold_status=PENDING`.

4. Silver processes only pending event dates.
   - Implemented pending-date selection from manifest.
   - Default max dates per run is `MAX_SILVER_DATES_PER_RUN=7`.
   - Silver reads only existing Bronze partitions for selected dates.
   - Silver deletes matching valid/invalid Silver output partitions and writes
     replacements by `event_date`.
   - Silver marks `silver_status=DONE` only after partition replacement succeeds.

5. Gold incremental processes bounded pending dates.
   - Implemented `MAX_GOLD_DATES_PER_RUN`, default `3`.
   - `gold_prepare_events` claims dates where `silver_status=DONE` and
     `gold_status` is `PENDING` or retryable `RUNNING`, then reads only those
     Silver partitions.
   - Staging replaces selected `event_date` partitions.
   - Facts replace affected fact rows by `event_date` and `sale_date`.
   - Dimensions replace `dim_time` by `event_date` and replace product/user/
     session dimensions by affected keys recomputed from current Gold data.
   - Daily summaries recompute and replace only affected `event_date`
     partitions.
   - `refresh_gold_metadata` and `validate_gold_metadata` run after successful
     Gold data tasks.
   - `mark_gold_done` runs last, after fact, dimension, summary, and metadata
     tasks all succeed.

6. Full refresh remains admin/manual.
   - `gold_pipeline` defaults to incremental.
   - Admin full refresh requires `dag_run.conf` mode:

```json
{"mode": "full_refresh"}
```

   - Full refresh should be used for rebuilds, backfills, schema changes, or
     business logic changes.

## Files to change

Implemented files:

- `docs/DATA_ETL_BATCH_PROCESSING.md`
- `README.md`
- `code/kafka/producer.py`
- `script/kafka/producer.sh`
- `code/spark/common/config.py`
- `code/spark/common/partition_state.py`
- `code/spark/bronze_job.py`
- `code/spark/silver_job.py`
- `code/spark/gold/config.py`
- `code/spark/gold/writers.py`
- `code/spark/gold/tasks/gold_prepare_events.py`
- `code/spark/gold/tasks/gold_build_facts.py`
- `code/spark/gold/tasks/gold_build_dimensions.py`
- `code/spark/gold/tasks/gold_build_summaries.py`
- `code/spark/gold/tasks/gold_build_metadata.py`
- `code/spark/gold/tasks/gold_validate_metadata.py`
- `code/spark/gold/tasks/gold_mark_done.py`
- `code/airflow/dags/bronze_pipeline.py`
- `code/airflow/dags/silver_pipeline.py`
- `code/airflow/dags/gold_pipeline.py`
- `code/airflow/dags/AIRFLOW_DAGS.md`
- `code/spark/SPARK.md`
- `code/kafka/KAFKA.md`

Still future:

- `init/01_init_schemas.sh`: add PostgreSQL `etl_control` schema/table/grants
  when replacing manifest state.
- `envs/iceberg.env` or `envs/airflow.env`: add control DB env if not reusing
  existing PostgreSQL variables.
- `code/spark/common/config.py`: expose control DB/JDBC settings or a small
  control config object.
- New PostgreSQL helper, for example `code/spark/common/etl_control.py` or
  `code/spark/control/event_date_status.py`: claim/update statuses atomically.
- `code/airflow/dags/gold_metadata_pipeline.py`: still manual/admin for metadata
  only; the main Gold DAG now runs the same metadata build/validation after data
  success.
- `code/airflow/dags/dag_common.py`: no new control DB env was needed yet.

Do not change:

- `code/agent/`
- LangGraph nodes
- Text-to-SQL, metadata service, Trino service, guardrail code

## Airflow Trigger Strategy

- Bronze:
  - Keep scheduled every 10 minutes or manual trigger.
  - It should read Kafka from stored offsets and write only newly consumed data.
  - Keep `max_active_runs=1` to protect offset updates.

- Silver:
  - Preferred: trigger after Bronze success once a combined DAG or dataset-based
    dependency is introduced.
  - Acceptable interim: keep short schedule, but process only
    `bronze_status='DONE' AND silver_status='PENDING'`.
  - Keep `max_active_runs=1` until atomic claim logic is proven.

- Gold incremental:
  - `gold_pipeline` is manual by default in Airflow (`schedule=None`) and runs
    incremental unless `dag_run.conf.mode` is set to `full_refresh`.
  - After runtime validation, it can be scheduled after Silver or run on a short
    cadence.
  - Each run processes at most `MAX_GOLD_DATES_PER_RUN` dates, default `3`.

- Gold full refresh:
  - Manual/admin only.
  - Use for rebuild, backfill, schema change, or business-logic change.
  - It should not be the default run path.

- Metadata refresh:
  - The main `gold_pipeline` now runs metadata build and validation after Gold
    data tasks succeed.
  - Keep standalone manual DAG for schema/semantic metadata updates.
  - In the incremental DAG, place metadata refresh before `mark_gold_done`.

## Idempotency Strategy

- Use `event_date` as the official batch key from Silver onward.
- Bronze:
  - Kafka offsets prevent re-consuming already committed Kafka messages.
  - Offset commit must happen after Bronze write and affected-date status update.
  - Bronze append may contain duplicate business events if the same source file is
    produced again; downstream dedup/partition replacement must handle this.
- Silver:
  - For a claimed `event_date`, write output by replacing the entire Silver
    partition, not by blind append.
  - Deduplicate by `event_fingerprint` within the replacement batch.
  - Mark Silver done only after write validation.
- Gold facts:
  - Replace affected `fact_events.event_date` partitions and
    `fact_sales.sale_date` partitions.
  - Do not append facts without deleting/replacing the same date first.
- Gold dimensions:
  - `dim_time` is replaced for active `event_date` values.
  - `dim_product`, `dim_user`, and `dim_session` are recomputed for keys present
    in the active incremental result, then replaced by key.
  - Limitation: if a reprocessed date removes every event for a previously
    affected product/user/session key, those aggregate dimensions may retain old
    contributions until an admin full refresh.
- Gold summaries:
  - Recompute summaries from facts for affected dates and replace those summary
    partitions.
- Status updates:
  - Current manifest backend claims Gold dates in `active_gold_run` and sets
    `gold_status=RUNNING` under Airflow `max_active_runs=1`.
  - Transition to `DONE` only after all writes and validations succeed.
  - Failed Silver/Gold dates are reset to `PENDING` with `last_error`, so
    scheduled/manual retries can pick them up again.

## Gold Incremental Strategy

1. Select pending dates.
   - Claim oldest dates where `silver_status='DONE'` and
     `gold_status='PENDING'`; stale `RUNNING` dates are treated as retryable.
   - Limit by `MAX_GOLD_DATES_PER_RUN`, default `3`.
   - Persist the claimed set in `active_gold_run`, set `gold_status='RUNNING'`,
     and store the Airflow `run_id` when available.

2. Prepare staging.
   - Read only Silver partitions for claimed dates.
   - Deduplicate by `event_fingerprint`.
   - Replace staging partitions for claimed dates.
   - If a selected date has zero valid Silver rows, delete any old staging rows
     for that date and continue with empty output.

3. Build facts.
   - Read the staging batch.
   - Replace `fact_events` partitions where `event_date` is in claimed dates.
   - Replace `fact_sales` partitions where `sale_date` is in claimed dates.
   - Validate uniqueness and purchase/sales count consistency for the batch.

4. Build dimensions.
   - Replace `dim_time` rows by active `event_date`.
   - Recompute `dim_product` by affected `product_id` from current staging.
   - For `dim_user`/`dim_session`, recompute affected keys from final facts after
     fact partition replacement, then replace by key.

5. Build summaries.
   - Read final facts/dimensions.
   - Filter to claimed dates.
   - Replace summary partitions for claimed dates.

6. Refresh metadata and mark done.
   - Run semantic metadata refresh/validation.
   - Mark claimed dates `gold_status='DONE'` only after every Gold and metadata
     task succeeds.
   - If a Gold data task or metadata task fails, reset active dates to
     `gold_status='PENDING'` and write `last_error`.

## Testing Plan

Static checks:

```bash
python -m py_compile code/spark/bronze_job.py
python -m py_compile code/spark/silver_job.py
python -m py_compile code/spark/common/config.py
python -m py_compile code/spark/common/partition_state.py
python -m py_compile code/kafka/producer.py
python -m py_compile code/airflow/dags/bronze_pipeline.py
python -m py_compile code/airflow/dags/silver_pipeline.py
python -m py_compile code/airflow/dags/gold_pipeline.py
python -m py_compile code/airflow/dags/gold_metadata_pipeline.py
python -m py_compile code/spark/gold/tasks/gold_mark_done.py
python -m compileall code/spark/gold
bash -n script/kafka/producer.sh
```

Airflow checks:

```bash
docker exec -it airflow airflow dags list
docker exec -it airflow airflow tasks list bronze_pipeline
docker exec -it airflow airflow tasks list silver_pipeline
docker exec -it airflow airflow tasks list gold_pipeline
docker exec -it airflow airflow tasks list gold_metadata_pipeline
```

Manifest state checks:

```bash
docker exec -it airflow env PYTHONPATH=/opt/project/code/spark python - <<'PY'
from pyspark.sql import SparkSession
from common.config import load_minio_config
from common.partition_state import read_state
from common.s3a import apply_s3a_options

builder = SparkSession.builder.appName("ReadPartitionState")
spark = apply_s3a_options(builder, load_minio_config()).getOrCreate()
state = read_state(spark, "s3a://bronze/_state/etl_partition_status.json")
for partition_date, entry in sorted(state.get("partitions", {}).items()):
    print(partition_date, entry)
spark.stop()
PY
```

Pending Silver dates in manifest:

```bash
docker exec -it airflow env PYTHONPATH=/opt/project/code/spark python - <<'PY'
from pyspark.sql import SparkSession
from common.config import load_minio_config
from common.partition_state import pending_silver_dates
from common.s3a import apply_s3a_options

builder = SparkSession.builder.appName("PendingSilverDates")
spark = apply_s3a_options(builder, load_minio_config()).getOrCreate()
print(pending_silver_dates(spark, "s3a://bronze/_state/etl_partition_status.json", 100))
spark.stop()
PY
```

Pending Gold dates in manifest:

```bash
docker exec -it airflow env PYTHONPATH=/opt/project/code/spark python - <<'PY'
from pyspark.sql import SparkSession
from common.config import load_minio_config
from common.partition_state import pending_gold_dates
from common.s3a import apply_s3a_options

builder = SparkSession.builder.appName("PendingGoldDates")
spark = apply_s3a_options(builder, load_minio_config()).getOrCreate()
print(pending_gold_dates(spark, "s3a://bronze/_state/etl_partition_status.json", 100))
spark.stop()
PY
```

Data correctness checks:

```sql
SELECT event_date, COUNT(*)
FROM iceberg.gold.fact_events
GROUP BY event_date
ORDER BY event_date;

SELECT sale_date, COUNT(*), SUM(gross_amount)
FROM iceberg.gold.fact_sales
GROUP BY sale_date
ORDER BY sale_date;

SELECT event_date, total_events, total_purchases, total_revenue
FROM iceberg.gold.daily_event_summary
ORDER BY event_date;
```

Idempotency checks:

- Run the same producer file twice.
- Run Bronze/Silver twice for the same affected dates.
- Verify Silver valid/invalid counts do not double.
- Reset one date from `DONE` to `PENDING`, rerun, and verify replacement counts
  remain stable.

Backfill/full-refresh checks:

- Trigger full refresh manually after backing up current state.
- Validate Gold counts and semantic metadata tables.
- Verify Agent still reads only Gold/metadata through Trino.

## Risks And Decisions

- Bronze currently partitions by Kafka timestamp date. Changing to parsed
  `event_date` changes physical layout and may require a one-time backfill or
  cleanup of old Bronze paths.
- Silver plain Parquet partition replacement is less robust than Iceberg MERGE.
  Keep single-writer Airflow settings until the write protocol is hardened.
- Existing Gold fact tables are not consistently created with date partitions.
  Partition-safe incremental writes may require a one-time full refresh/recreate.
- `dim_user` and `dim_session` are aggregate-heavy dimensions. Incremental
  correctness needs recompute-by-affected-key or schema simplification.
- Metadata docs have some historical names, but current code writes
  `semantic_table_catalog` and `semantic_column_catalog`.
- Failed runs need clear reset procedures so dates do not remain stuck in
  `RUNNING` or `FAILED`.
