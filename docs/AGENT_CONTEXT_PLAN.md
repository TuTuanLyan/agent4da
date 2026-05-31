# Conversational Context Plan — In-Session Memory for the Agent

Goal: make the v2 agent behave like a real assistant inside a chat session. It
should remember every prior turn, understand follow-ups that only make sense in
context ("bỏ qua nhãn hàng unknown", "còn năm 2021 thì sao", "thấp nhất thì
sao", "chi tiết của apple"), compound refinements across many turns, and still
fall back gracefully when the LLM is unavailable. Guardrails (read-only,
Gold-only, fact LIMIT, correction loop) never change.

This document is the design + phased plan. It is written against the current
`app/backend/agent/engine_v2` engine.

> **Status (implemented):** Phases 0–4 are implemented and tested offline.
> The Active Query Spec + `merge_spec` engine (`engine_v2/spec.py`),
> `classify_followup`/`llm_extract_patch` (`engine_v2/context.py`), the
> inclusion/threshold/`analyze_message` NLU detectors (`engine_v2/nlu.py`), the
> aggregate/HAVING/multi-metric SQL builder (`engine_v2/sql_generator.py`), and
> the graph routing (`engine_v2/graph.py`) are in place; durable spec/result
> rehydration is in `agent/service.py`. See the scenario matrix in
> `tests/test_engine_v2.py`. Goes live after `make app-rebuild`.

---

## 1. Where we are now

What already exists (do not rebuild, extend):

- **Two context stores.** Process-local ring buffer (`engine_v2/context.py`
  `save_turn`/`get_recent_context`) plus durable rehydration from `query_runs`
  (`agent/service.py` `recent_session_context`). The durable path makes context
  survive restarts/multi-worker. This is the right foundation.
- **A deterministic follow-up resolver** (`context.py` `resolve_followup`) that
  today handles a fixed set: chart reuse ("vẽ biểu đồ"), explain SQL, dimension
  switch (category/product), limit change ("top 5 thôi"), and exclusion
  ("bỏ qua X" -> `NOT IN`).
- **An LLM rewriter fallback** (`context.py` `llm_rewrite_followup`) that merges
  an elliptical message with recent turns into a standalone question.
- **A structured NLU** (`nlu.py` `parse_nlu`) that emits an `intent_result`
  (intent, dimension, metric, time_range/grain, filters, sort_direction, limit,
  comparison_entities, table_candidates, analysis_type, extracted_entities).
- **A deterministic SQL builder** (`sql_generator.py` `_deterministic_sql`) per
  intent, with an LLM SQL path (`generate_sql(prefer_llm=...)`).

The weakness: follow-ups are matched by ad-hoc keyword rules, each added by hand.
There is no single model of "what the conversation is currently about", so
refinements do not compose and unanticipated phrasings fall through.

---

## 2. Core design — a Session Analytical State + delta resolution

The spine of the plan is one idea:

> Maintain, per session, a structured **Active Query Spec** (the resolved
> `intent_result` of the last successful analytical turn). Treat each new turn as
> one of a small number of **operations** on that spec, not as an isolated string.

Every turn is classified into exactly one operation:

| Op | Meaning | Effect on Active Spec |
|----|---------|-----------------------|
| `new_query` | A self-contained analytical question | Replace the spec |
| `refine` | A delta on the current spec (filter, time, metric, dim, sort, limit) | Merge patch into spec, regenerate SQL |
| `presentation` | Reuse the prior result, change how it is shown (chart, explain, format) | No new query |
| `entity_ref` | Question about entities from the prior result ("cái đầu tiên", "apple đó", "những cái còn lại") | Derive filter/drilldown from prior result rows |
| `clarification_answer` | Answer to a question/suggestion the agent just asked | Adopt the chosen intent |
| `meta` | Talk about the conversation itself ("câu trước hỏi gì", "bạn làm được gì") | Conversational answer, spec unchanged |
| `reset` | Abandon context ("thôi", "quên đi", "chuyển chủ đề") | Clear the spec, treat next as new |

Because the merged spec becomes the **new** Active Spec after each turn,
refinements compound naturally:

```
T1 "top brand theo view 2020"     spec = {rank, brand, views, year=2020, limit=10}
T2 "bỏ qua unknown"               spec += filter(brand NOT IN unknown)
T3 "chỉ top 5"                    spec.limit = 5
T4 "đổi sang doanh thu"           spec.metric = revenue
T5 "còn 2021 thì sao"             spec.time = 2021
```

Each turn regenerates SQL from the current spec. This is what "remembers all the
context in the same session" means concretely.

### Why a structured spec (not string-merging)

String-merging ("previous question" + "new message") works for simple cases but
breaks on compounding (T3 onward), on corrections ("không, 2021 không phải
2020"), and is hard to validate. A typed spec + typed patch is precise,
composable, and lets us validate every column/table against semantic metadata
before SQL generation — so the LLM can never inject a hallucinated column.

---

## 3. Taxonomy of follow-up situations to support

For each: detection signal, resolution, and which layer owns it
(D = deterministic rules, L = LLM, both = D with L fallback). Examples are the
Vietnamese/English phrasings users actually type.

### 3.1 Filter refinements (`refine`)
- Exclusion — "bỏ qua / loại trừ / không tính / ngoại trừ X" -> `NOT IN`. **[D, done]**
- Inclusion / restriction — "chỉ X", "chỉ brand apple và samsung", "riêng category điện thoại" -> `IN` / `=`. **[D]**
- Numeric threshold — "chỉ những cái > 1000 views", "doanh thu trên 1 triệu", "ít nhất 100 đơn" -> `WHERE`/`HAVING` numeric. **[D]**
- Remove a filter — "bỏ điều kiện 2020 đi", "không lọc brand nữa" -> drop a filter from the spec. **[both]**

### 3.2 Time changes (`refine`)
- Absolute switch — "năm 2021", "tháng 1/2020", "quý 2 2020", "nửa đầu 2020" -> replace `time_range`. **[D]**
- Relative — "tháng trước", "30 ngày gần nhất", "tuần này", "hôm qua" -> symbolic range (some exist). **[D]**
- Widen/narrow — "cả năm", "chỉ tháng 3" -> replace range. **[D]**
- Compare periods — "so với 2019", "so với tháng trước" -> dual-range comparison. **[L first, D for the common YoY/MoM]**

### 3.3 Dimension / grouping (`refine`)
- Switch dimension — "còn category thì sao", "theo sản phẩm", "theo brand". **[D, partial]**
- Add a breakdown dimension — "chia theo brand và category". **[L first]**
- Drill down — "chi tiết sản phẩm của apple", "các ngày trong tháng đó". **[both]**
- Roll up — "gộp theo tháng", "tổng theo brand". **[D]**

### 3.4 Metric changes (`refine`)
- Switch metric — "đổi sang doanh thu", "theo lượt mua thay vì xem". **[D]**
- Add metric — "thêm cột doanh thu", "kèm conversion rate" (multi-metric output). **[D]**
- Derived/ratio — "tỷ lệ chuyển đổi", "trung bình mỗi ngày", "% đóng góp". **[L first]**

### 3.5 Ranking / order / limit (`refine`)
- Limit — "top 5 thôi", "top 20", "10 cái đầu". **[D, done]**
- Direction — "thấp nhất thay vì cao nhất", "ngược lại". **[D]**
- Re-sort by another column — "sắp theo doanh thu". **[D]**

### 3.6 Presentation on prior result (`presentation`)
- Chart — "vẽ biểu đồ" (reuse, done), "đổi sang line/pie", "biểu đồ cột". **[D]**
- Explain — "giải thích SQL", "tại sao", "câu query vừa rồi làm gì". **[D, done]**
- Format — "hiển thị %", "làm tròn", "đổi đơn vị". **[D]**
- Export hint — "xuất CSV" (frontend action; agent just confirms columns). **[D]**

### 3.7 Entity references to the prior result (`entity_ref`)
- Ordinal/positional — "cái đầu tiên", "top 1", "3 cái cuối", "những cái còn lại". **[D from stored rows]**
- Named entity from prior result — "chi tiết của apple", "so sánh 2 cái đầu". **[both]**
- Pronoun/deixis — "nó", "chúng", "those", "that one". **[L]**
- Requires storing the prior result rows (see Section 4).

### 3.8 Comparison / diagnostic follow-on (`refine`/`meta`)
- "tăng hay giảm so với kỳ trước", "xu hướng thế nào" -> trend/comparison spec. **[L first]**
- "tại sao apple cao thế" -> explanation grounded in the result (no fabricated cause). **[L]**

### 3.9 Clarification answers (`clarification_answer`)
- The previous turn returned `needs_clarification=true` with suggestion chips
  (each carries an intent + a full question). The user replies "brand" or clicks
  a chip -> adopt that suggestion's question/intent as the resolved query. **[D]**

### 3.10 Meta / conversational (`meta`)
- "câu trước hỏi gì", "bạn làm được gì", "dữ liệu có những bảng nào", thanks/greetings.
  Answered conversationally (existing `conversation.py` assistant), spec untouched. **[L, with D fallback]**

### 3.11 Reset / topic change (`reset`)
- "thôi chuyển câu khác", "quên cái trên đi", or a fully self-contained new
  question with its own dimension+metric -> clear Active Spec. **[D]**
- Correction — "không phải 2020, là 2021" -> a `refine` that *replaces* (not adds). **[both]**

---

## 4. Data model: what each turn must remember

Most of this is already persisted; the work is to **surface it** into
`recent_context` and add a couple of fields.

Per successful turn, the session context record should carry:

1. `question` (raw user text) — already stored.
2. `effective_spec` — the resolved `intent_result` actually used (dimension,
   metric(s), time_range, time_grain, filters, sort, limit, table, analysis_type).
   `agent_trace` already stores almost all of these; add the full resolved spec
   as one field so we never have to reparse.
3. `generated_sql` — already stored.
4. `result_columns` + `result_sample` — top ~20 rows of the result, for
   `entity_ref` resolution ("cái đầu tiên", "apple đó"). `query_runs.result_json`
   already holds up to 10k rows; surface the first N into context.
5. `chart` payload — already stored (for chart reuse).
6. `clarification` — pending suggestions/question if the turn asked for
   clarification, so the next turn can be read as an answer.
7. `status`, `intent`, `answer` — already stored.

Changes:
- `engine_v2/context.py` `save_turn`: also store `effective_spec`,
  `result_columns`, `result_sample`, `clarification`.
- `agent/service.py` `recent_session_context` / `_query_run_to_context_turn`:
  surface `effective_spec` (from `agent_trace`), `result_sample`/`result_columns`
  (from `result_json`/`columns`), and clarification (from `agent_trace`).
- Keep the size bounded (last 5 turns, <=20 sample rows/turn) so prompts stay small.

No schema migration is strictly required — `agent_trace` (JSONB) and
`result_json` already exist; we are reading more of them and writing a slightly
richer trace.

---

## 5. Resolution pipeline (graph changes)

Replace the single `resolve_followup` step with a small, explicit sub-pipeline
between `load_context` and `intent_router`:

```
load_context
  -> classify_turn        # op = new_query | refine | presentation | entity_ref
  |                       #      | clarification_answer | meta | reset
  -> apply_context        # build effective_question / effective_spec per op
  -> intent_router ...    # unchanged downstream
```

Components:

- **`classify_turn` (new, `context.py`)**: deterministic first.
  1. Compute `nlu_self = parse_nlu(message)` (what the message says on its own).
  2. If `nlu_self` is a complete standalone analytical question (has dimension
     and/or metric and is not dominated by a refine cue) -> `new_query`.
  3. Else inspect cue lexicons (exclude/include/threshold/time/metric/dim/sort/
     limit/chart/explain/ordinal/pronoun/reset/meta) -> the matching op + a typed
     **patch**.
  4. If still ambiguous and the LLM is available -> one JSON classification call
     (`{op, patch}`) grounded in the Active Spec + recent turns + semantic
     metadata. Validate the patch against metadata; drop anything unknown.
  5. If no LLM and ambiguous -> fall back to the existing `llm_rewrite_followup`
     (string rewrite) or, last resort, treat as `new_query`.

- **`apply_context` (new, `context.py`)**:
  - `refine`/`entity_ref`/`clarification_answer` -> `merged_spec =
    merge(active_spec, patch)`; set `effective_spec` and either (a) build SQL
    deterministically from `merged_spec`, or (b) when the patch is open-ended,
    set `effective_question` + `prefer_llm` with the prior SQL as grounding.
  - `presentation` -> reuse prior result/SQL (existing chart/explain paths).
  - `meta` -> route to the conversational assistant (existing).
  - `reset`/`new_query` -> clear/replace Active Spec; run normally.

- **`merge(spec, patch)` (new, pure function)**: field-wise apply
  (replace vs append). Filters append unless the patch targets the same field
  (then replace — handles corrections). Recompute `table_candidates` when the
  dimension changes. This is the heart and is fully unit-testable offline.

- **`sql_generator.py`**: extend `_deterministic_sql` so every spec field is
  honored: inclusion/threshold filters (Section 3.1), multi-metric SELECT
  (3.4 add-metric), `HAVING` for aggregate thresholds, sort-by-arbitrary-column,
  comparison/period-over-period. Reuse the existing `_filter_conditions`,
  `_aggregate_expr`, availability guard.

The Active Spec lives in the session context store (process + durable), updated
in `save_context`.

---

## 6. LLM usage — structured, validated, optional

The LLM is an accelerator, never a dependency:

- **Structured delta extraction** (preferred LLM use): when rules are unsure, ask
  the LLM for a *typed patch* (`{op, set:{...}, add_filters:[...],
  remove_filters:[...]}`) as JSON, not free-form SQL or prose. We then validate
  every referenced table/column/metric against semantic metadata and discard
  unknowns. This keeps the LLM from hallucinating schema while still
  understanding messy phrasing.
- **Rewrite fallback**: the existing `llm_rewrite_followup` stays as a final
  fallback for cases the patch model cannot express.
- **Conversational answers**: `conversation.py` handles `meta` and out-of-scope.
- **No-key degradation**: every LLM step has a deterministic fallback; with no
  `GROQ_API_KEY` the agent still does exclusions, time/metric/dim/limit/sort
  changes, presentation, ordinal entity refs, and clarification answers. Only the
  fuzzy long tail (pronouns, free-form derived metrics) needs the key.

---

## 7. Safety: topic change, reset, correction

- **Topic-change detection** prevents context bleed: if the new message is a
  complete standalone question on a different dimension+metric, it is `new_query`
  and the old spec is dropped. Conservative default: when in doubt between
  `refine` and `new_query`, prefer `new_query` unless an explicit refine cue is
  present — wrong context is worse than no context.
- **Reset cues** ("thôi", "quên đi", "câu khác", "bắt đầu lại") clear the spec.
- **Correction cues** ("không phải", "ý tôi là", "sửa lại") force *replace*
  semantics on the targeted field.
- All resolved SQL still passes `guard_sql` (read-only, Gold-only, LIMIT) and the
  correction/retry loop. Context resolution can change *what* is asked, never
  *whether* the guardrails apply.

---

## 8. Entity-reference resolution (Section 3.7 detail)

- Store `result_sample` (top rows) + `result_columns` per turn (Section 4).
- Deterministic for positional refs: "cái đầu tiên/top 1" -> row[0]'s dimension
  value; "những cái còn lại" -> all dimension values except the previously shown
  top; "3 cái cuối" -> last 3. Turn these into an `IN`/`NOT IN` patch.
- LLM for named/fuzzy refs ("apple đó", "nó"): resolve the referent against the
  stored sample, emit a filter patch, validate the value exists in the sample.
- Never invent entity values not present in the prior result.

---

## 9. Testing strategy

- **Scenario matrix** (offline, no Trino/LLM): multi-turn conversations as
  fixtures; assert the resolved SQL (or op + patch) per turn. Cover every row in
  Section 3, plus compounding chains (T1..T5 above), corrections, resets, and
  topic changes (must NOT carry context).
- **Merge unit tests**: `merge(spec, patch)` for replace-vs-append, dimension
  change recomputing tables, filter de-duplication.
- **LLM-path tests**: mock `chat_completion` to return patches/rewrites; assert
  validation drops unknown columns.
- **Regression**: keep all 70 current tests green; the new pipeline must not
  change single-turn behavior.
- **Guard tests**: every generated SQL across the matrix passes `validate_sql`.

---

## 10. Phased rollout (recommended order)

Each phase is shippable and independently testable.

- **Phase 0 — Surface what we already store (low risk, high leverage).**
  Put `effective_spec`, `result_sample`, and `clarification` into
  `recent_context` (Section 4). No behavior change yet; unblocks everything else.

- **Phase 1 — Active Spec + `merge` + deterministic refines.**
  Add the spec carryover, `classify_turn`/`apply_context`/`merge`, and extend the
  SQL builder for inclusion + threshold + sort-direction + limit + time-switch +
  metric switch/add. Covers the bulk of Section 3.1-3.5 with no LLM. This is the
  biggest correctness win and makes the exact reported scenario plus its near
  neighbors all work.

- **Phase 2 — Presentation + clarification-answer + reset/correction.**
  Generalize chart/explain/format; resolve suggestion answers; handle reset and
  correction semantics (Sections 3.6, 3.9, 3.11, 7).

- **Phase 3 — Entity references.**
  Positional refs deterministically; named/pronoun refs via LLM over the stored
  sample (Section 8).

- **Phase 4 — LLM structured delta extractor + comparison/derived metrics.**
  Replace the ambiguous-case rewriter with the validated patch model; add
  period-over-period and ratio metrics (Sections 3.2 compare, 3.4 derived, 3.8).

- **Phase 5 — Hardening.**
  Full scenario matrix, topic-change tuning, prompt-size budget, telemetry on
  which op each turn resolved to (stored in `agent_trace` for debugging).

---

## 11. Risks & mitigations

- **Context bleed** (treating a new question as a refine): conservative
  `new_query` default + explicit refine cues; log the chosen op.
- **Compounding drift** (spec accumulates stale filters): reset cues, correction
  semantics, and "remove filter" support; show applied filters in the answer so
  the user can see/clear them.
- **LLM hallucinating schema**: typed patches validated against semantic
  metadata; unknown columns/tables dropped before SQL.
- **Prompt bloat**: cap to last 5 turns and <=20 sample rows; only include the
  spec + a compact result sample, not full rows.
- **Ambiguous references with large/!unique results**: ask one clarification
  rather than guess.
- **Backward compatibility**: single-turn behavior unchanged; all changes gated
  behind "is there an Active Spec / recent context".

---

## 12. Operational note

The backend image **bakes in `app/backend`** (compose mounts only `./code` and
`./envs`). Any change here goes live only after `make app-rebuild`. The
process-local store is per-replica; the durable `query_runs` rehydration is what
makes context correct across restarts and multiple workers — keep durable
context as the source of truth and the process store as a same-process cache.

---

## 13. Definition of done

A user can, in one session: ask an analytical question; then issue a chain of
refinements (exclude, restrict, change time, swap/add metric, change
dimension/sort/limit), reference prior-result entities, ask for a chart or
explanation, correct themselves, and start a fresh topic — and the agent resolves
each turn correctly, deterministically where possible, with read-only guardrails
intact, and degrades sanely without the LLM.
