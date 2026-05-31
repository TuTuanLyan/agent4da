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
-> resolve_followup   (deterministic fast-paths + LLM follow-up rewrite)
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

## Conversational Context (Follow-ups)

The engine maintains an **Active Query Spec** per session — the resolved
`intent_result` (dimension, metric(s), time range, filters, sort, limit, table)
of the last successful analytical turn. `engine_v2/context.classify_followup`
maps each new turn to one operation, resolved as a typed delta on that spec, so
refinements **compound** across many turns:

| Op | Examples | Effect |
|----|----------|--------|
| `new_query` | a complete standalone question | replace the spec |
| `refine` | "bỏ qua unknown", "chỉ apple", "trên 1000 view", "còn 2021", "đổi sang doanh thu", "thêm doanh thu", "theo category", "thấp nhất", "top 5 thôi" | merge a patch (set / add_filters / remove_filter_fields / add_metrics), rebuild SQL |
| `entity_ref` | "cái đầu tiên", "những cái còn lại", "chi tiết của samsung" | derive `IN`/`NOT IN` from the prior result rows |
| `presentation` | "vẽ biểu đồ", "giải thích SQL" | reuse the prior result, no new query |
| `clarification_answer` | a short reply to an offered suggestion | adopt that suggestion |
| `meta` | "bạn làm được gì", greetings | conversational answer; spec unchanged |
| `reset` | "thôi chuyển chủ đề", "quên đi" | drop the spec |
| `ambiguous` | elliptical, no detectable delta | LLM typed-patch (`llm_extract_patch`) → else `llm_rewrite_followup` |

`merge_spec` (`engine_v2/spec.py`) applies the patch: exclusions accumulate on
the same field, inclusions/numerics replace same field+operator, a metric switch
resets the metric list, a dimension switch re-derives the table candidates. The
merged spec runs directly (skipping a fresh NLU parse). Refinements are
deterministic and need no Groq key; the LLM only handles the ambiguous tail and
returns a *validated* patch (allow-listed fields/metrics/operators). The SQL
builder also drops any column absent from the chosen table, so a brand filter
never leaks onto `daily_event_summary`.

Supporting NLU/SQL: the NLU extracts exclusion (`NOT IN`), inclusion (`IN`),
numeric thresholds (`HAVING`), and bare-year ranges ("trong 2020"); ranking
queries aggregate (`SUM`/`AVG` + `GROUP BY`, multi-metric aware) so totals reflect
the whole period and filters apply before grouping. The same read-only guard,
Gold-only allow-list, and correction loop apply to every resolved query.

Context source: prior turns (question, SQL, the spec, and a small result sample)
are rehydrated from durable `query_runs` by `agent.service.recent_session_context`
(newest-first, user-scoped) and passed into the graph, so resolution survives a
restart or multi-worker routing. The in-process store in `engine_v2/context.py`
is a same-process fast path used when no durable context is supplied.

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
