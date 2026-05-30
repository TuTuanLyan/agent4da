# AI Backend Flow Update

## Canonical Data Contract

- Raw source: Kaggle/REES46 e-commerce behavior events.
- ETL path: raw input -> Bronze on MinIO -> Silver Parquet -> Gold Apache Iceberg -> Trino -> AI Agent.
- Spark/Iceberg uses the JDBC catalog name `iceberg_catalog`.
- Backend/Trino exposes the runtime catalog as `iceberg`, so Agent SQL must use `iceberg.gold.*` and `iceberg.metadata.*`.
- Agent reads only Gold business tables and semantic metadata. It never reads Bronze or Silver in the normal Ask flow.

Gold tables visible to the Agent:

- `fact_events`, `fact_sales`
- `dim_time`, `dim_product`, `dim_user`, `dim_session`
- `daily_event_summary`, `daily_product_summary`, `daily_category_summary`, `daily_brand_summary`
- `iceberg.metadata.semantic_table_catalog`, `iceberg.metadata.semantic_column_catalog`

## Agent v2 Flow

The backend engine is `app/backend/agent/engine_v2` and runs through FastAPI in `app/backend`.

```text
input
-> load_context
-> safety
-> nlu
-> metadata
-> ambiguity_check
-> text_to_sql
-> guard_sql
-> execute_sql
-> validate_result
-> correct_sql retry loop
-> chart
-> insight
-> suggestion_generation
-> save_context
-> output
```

Guardrails remain mandatory: read-only SQL, Gold-only tables, metadata allow-list, fact/detail LIMIT, and max 3 correction attempts.

## Clarification Behavior

The Agent no longer returns the hard unsupported sentence. Ambiguous, empty, blocked, or outside-source requests return:

- `answer_type`
- `needs_clarification`
- `clarification_suggestions`
- `assumptions`

Suggestions are generated from the current input, NLU result, conversation context, result status, and Gold metadata. The frontend renders these backend suggestions and does not use hard-coded fallback chips for failed/empty runs.

## Contextual Learning

V1 learning is contextual, not fine-tuning:

- `app.agent_suggestion_events` stores generated suggestions per run.
- `app.agent_feedback` stores suggestion clicks and explicit feedback.
- Future ranking can use successful query patterns and clicked suggestions, while guardrails and allowed tables remain fixed.
