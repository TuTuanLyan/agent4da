"""Offline unit tests for the v2 agent engine.

These cover the pure, infra-free logic: SQL guard, NLU, corrector heuristics,
result validation, chart selection, rule-based insights + LLM fallback, and the
response->state mapping. No Trino/Groq/Postgres needed.

Run from app/backend:  python -m pytest tests/test_engine_v2.py
"""

from __future__ import annotations

import pytest

from agent.engine_v2 import (
    charts,
    context,
    conversation,
    corrector,
    guard,
    insights,
    nlu,
    runner,
    spec as spec_mod,
    sql_generator,
    suggestions,
    validator,
)
from agent.engine_v2.config import GOLD_PREFIX


# --- SQL guard --------------------------------------------------------------

def test_guard_allows_select_and_qualifies_catalog():
    out = guard.validate_sql("SELECT brand, revenue FROM iceberg.gold.daily_brand_summary")
    assert out.lower().startswith("select")
    assert "iceberg.gold.daily_brand_summary" in out
    # Non-aggregate select gets an auto LIMIT.
    assert "limit" in out.lower()


def test_guard_blocks_ddl_dml():
    for sql in (
        "DROP TABLE iceberg.gold.fact_events",
        "DELETE FROM iceberg.gold.fact_events",
        "INSERT INTO iceberg.gold.fact_events VALUES (1)",
        "UPDATE iceberg.gold.fact_events SET price = 0",
    ):
        with pytest.raises(ValueError):
            guard.validate_sql(sql)


def test_guard_blocks_foreign_catalog_and_schema():
    for sql in (
        "SELECT * FROM postgresql.gold.fact_events LIMIT 10",
        "SELECT * FROM bronze.events LIMIT 10",
        "SELECT * FROM silver.events LIMIT 10",
        "SELECT * FROM analytics_test.fact_events LIMIT 10",
        "SELECT * FROM iceberg_catalog.gold.fact_events LIMIT 10",
    ):
        with pytest.raises(ValueError):
            guard.validate_sql(sql)


def test_guard_requires_limit_on_fact_unless_aggregate():
    with pytest.raises(ValueError):
        guard.validate_sql("SELECT event_id FROM iceberg.gold.fact_events")
    # Aggregate is fine without LIMIT.
    out = guard.validate_sql("SELECT count(*) FROM iceberg.gold.fact_events")
    assert "count(*)" in out.lower()


def test_guard_rejects_multistatement():
    with pytest.raises(ValueError):
        guard.validate_sql("SELECT 1 FROM iceberg.gold.dim_time; SELECT 2 FROM iceberg.gold.dim_user")


def test_guard_metadata_requires_gold_filter():
    bad = "SELECT column_name FROM iceberg.information_schema.columns"
    with pytest.raises(ValueError):
        guard.validate_sql(bad)
    good = (
        "SELECT column_name, data_type FROM iceberg.information_schema.columns "
        "WHERE table_schema = 'gold' AND table_name = 'fact_events'"
    )
    out = guard.validate_sql(good)
    assert "metadata.semantic_column_catalog" in out.lower()
    assert "is_agent_visible = true" in out.lower()
    assert "gold.fact_events" in out.lower()

    qualified = (
        "SELECT column_name, data_type FROM iceberg.information_schema.columns "
        "WHERE table_schema = 'gold' AND table_name = 'gold.fact_events'"
    )
    out = guard.validate_sql(qualified)
    assert "metadata.semantic_column_catalog" in out.lower()
    assert "gold.fact_events" in out.lower()

    semantic = (
        "SELECT table_name FROM iceberg.metadata.semantic_table_catalog "
        "WHERE is_agent_visible = true"
    )
    out = guard.validate_sql(semantic)
    assert "metadata.semantic_table_catalog" in out.lower()


def test_guard_allows_show_and_describe():
    show_sql = guard.validate_sql(f"SHOW TABLES FROM {GOLD_PREFIX}")
    assert "metadata.semantic_table_catalog" in show_sql.lower()
    assert guard.validate_sql(f"DESCRIBE {GOLD_PREFIX}.fact_events").lower().startswith("describe")


# --- NLU --------------------------------------------------------------------

def test_nlu_ranking():
    result = nlu.parse_nlu("Top 5 brand nào có doanh thu cao nhất?")
    assert result["intent"] in {"ranking", "revenue_sales"}
    assert result["limit"] == 5
    assert result["sort_direction"] == "desc"


def test_nlu_trend():
    result = nlu.parse_nlu("Doanh thu theo ngày như thế nào?")
    assert result["intent"] in {"trend", "revenue_sales"}
    assert result["time_grain"] == "day"


def test_nlu_metadata_tables():
    result = nlu.parse_nlu("Hệ thống có những bảng gold nào?")
    assert result["intent"] == "metadata_tables"


def test_nlu_metadata_columns():
    result = nlu.parse_nlu("Bảng fact_events có những cột nào?")
    assert result["intent"] == "metadata_columns"
    assert result["table_name"] == "fact_events"


def test_nlu_unsupported():
    result = nlu.parse_nlu("Thời tiết Hà Nội hôm nay thế nào?")
    assert result["intent"] == "unsupported"


def test_nlu_exact_date():
    result = nlu.parse_nlu("Doanh thu trong ngày 2020-01-15")
    assert result["time_range"]["type"] == "exact_date"
    assert result["time_range"]["start"] == "2020-01-15"


def test_nlu_recognizes_nhan_hang_as_brand():
    # "nhãn hàng" is a common Vietnamese word for brand; it must map to the brand
    # dimension (and a view ranking), like "hãng"/"thương hiệu" already do.
    result = nlu.parse_nlu("Nhãn hàng nào có số lượt xem nhiều nhất trong 2020")
    assert result["dimension"] == "brand"
    assert result["metric"] == "total_views"
    assert result["intent"] == "ranking"
    assert result["sort_direction"] == "desc"


def test_nlu_nhan_does_not_misfire_on_nhanh():
    # Bare ASCII "nhan" is intentionally excluded so "nhanh" (fast) is not brand.
    result = nlu.parse_nlu("Doanh thu tang nhanh nhat vao ngay nao")
    assert result["dimension"] != "brand"


# --- Corrector --------------------------------------------------------------

def test_corrector_rewrites_disallowed_catalog():
    result = corrector.correct_sql(
        question="doanh thu",
        intent_result={"intent": "revenue_sales"},
        failed_sql="SELECT revenue FROM postgresql.gold.daily_brand_summary LIMIT 10",
        error_message="Catalog 'postgresql' not allowed",
        table_candidates=["daily_brand_summary"],
        metadata_context={"tables": ["daily_brand_summary"], "columns": {}},
        attempt_number=1,
    )
    assert result["can_retry"] is True
    assert f"{GOLD_PREFIX}.daily_brand_summary" in result["corrected_sql"]
    assert "postgresql" not in result["corrected_sql"].lower()


def test_corrector_replaces_unresolved_column():
    result = corrector.correct_sql(
        question="doanh thu",
        intent_result={"intent": "revenue_sales"},
        failed_sql="SELECT total_revenue FROM iceberg.gold.daily_brand_summary LIMIT 10",
        error_message="Column 'total_revenue' cannot be resolved",
        table_candidates=["daily_brand_summary"],
        metadata_context={
            "tables": ["daily_brand_summary"],
            "columns": {"daily_brand_summary": [{"name": "revenue", "type": "double"}]},
        },
        attempt_number=1,
    )
    assert result["can_retry"] is True
    assert "revenue" in result["corrected_sql"].lower()


def test_corrector_never_retries_ddl():
    result = corrector.correct_sql(
        question="x",
        intent_result={},
        failed_sql="DROP TABLE iceberg.gold.fact_events",
        error_message="Disallowed SQL keyword: DROP",
        table_candidates=["fact_events"],
        metadata_context={"tables": [], "columns": {}},
        attempt_number=1,
    )
    assert result["can_retry"] is False


def test_corrector_respects_retry_limit():
    result = corrector.correct_sql(
        question="x",
        intent_result={},
        failed_sql="SELECT 1 FROM iceberg.gold.dim_time",
        error_message="boom",
        table_candidates=["dim_time"],
        metadata_context={"tables": [], "columns": {}},
        attempt_number=corrector.MAX_SQL_RETRY_ATTEMPTS + 1,
    )
    assert result["can_retry"] is False


# --- Result validator -------------------------------------------------------

def test_validator_empty_rows_low_confidence():
    out = validator.validate_result(
        question="q", intent="ranking", generated_sql="SELECT ...",
        rows=[], row_count=0, table_candidates=[], used_tables=[],
    )
    assert out["is_empty"] is True
    assert out["confidence"] == "low"


def test_validator_revenue_zero_warns():
    out = validator.validate_result(
        question="q", intent="revenue_sales", generated_sql="SELECT total_revenue ...",
        rows=[{"total_revenue": 0}], row_count=1, table_candidates=[], used_tables=[],
    )
    assert any("0" in w or "doanh thu" in w.lower() for w in out["warnings"])
    assert out["confidence"] in {"medium", "low"}


# --- Chart recommender ------------------------------------------------------

def test_chart_ranking_is_bar():
    rows = [{"brand": "apple", "revenue": 100}, {"brand": "samsung", "revenue": 80}]
    chart = charts.recommend_chart(
        question="q", intent="ranking", rows=rows, row_count=2,
        generated_sql="", table_candidates=[], used_tables=[], warnings=[],
    )
    assert chart["recommended"] is True
    assert chart["type"] == "bar"
    assert chart["data"] == [{"x": "apple", "y": 100}, {"x": "samsung", "y": 80}]


def test_chart_trend_is_line():
    rows = [{"event_date": "2020-01-01", "total_revenue": 10}, {"event_date": "2020-01-02", "total_revenue": 20}]
    chart = charts.recommend_chart(
        question="q", intent="trend", rows=rows, row_count=2,
        generated_sql="", table_candidates=[], used_tables=[], warnings=[],
    )
    assert chart["type"] == "line"


def test_chart_drilldown_is_table():
    chart = charts.recommend_chart(
        question="q", intent="drilldown", rows=[{"event_id": 1}], row_count=1,
        generated_sql="", table_candidates=[], used_tables=[], warnings=[],
    )
    assert chart["recommended"] is False


# --- Insights ---------------------------------------------------------------

def test_rule_based_ranking_uses_dung_dau():
    out = insights.generate_insight(
        question="q", intent="ranking",
        rows=[{"brand": "apple", "revenue": 100}], warnings=[],
        generated_sql="", table_candidates=[], used_tables=[],
    )
    assert "đứng đầu" in out["answer"]


def test_llm_insight_falls_back_without_key(monkeypatch):
    # No Groq key -> must use the rule-based fallback, never raise.
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    out = insights.generate_llm_insight(
        question="q", intent="ranking",
        rows=[{"brand": "apple", "revenue": 100}], row_count=1, warnings=[],
        generated_sql="", chart={}, confidence="high",
        table_candidates=[], used_tables=[],
    )
    assert out["insight_source"] == "rule_based"
    assert out["llm_insight_used"] is False
    assert "đứng đầu" in out["answer"]


# --- Dynamic suggestions ----------------------------------------------------

def test_unsupported_answer_is_assistive_not_hard_rejection():
    out = insights.generate_insight(
        question="q", intent="unsupported",
        rows=[], warnings=[], generated_sql="", table_candidates=[], used_tables=[],
    )
    legacy_rejection = " ".join(["nằm", "ngoài", "phạm", "vi"])
    assert legacy_rejection not in out["answer"]
    assert "ngữ cảnh" in out["answer"]


def test_dynamic_suggestions_for_ambiguous_revenue():
    state = {
        "question": "Doanh thu sao rồi?",
        "intent": "revenue_sales",
        "metric": "revenue",
        "dimension": None,
        "time_range": None,
        "status": "success",
        "row_count": 1,
        "recent_context": [],
    }
    assumptions = suggestions.build_assumptions(state)
    chips = suggestions.build_suggestions({**state, "assumptions": assumptions})
    questions = [item["question"] for item in chips]
    assert assumptions
    assert len(chips) >= 3
    assert any("ngày gần nhất" in q for q in questions)
    assert any("brand" in q.lower() for q in questions)
    assert any("category" in q.lower() for q in questions)
    assert suggestions.needs_clarification(state, assumptions, chips) is True


def test_dynamic_suggestions_for_blocked_request_are_read_only():
    chips = suggestions.build_suggestions(
        {
            "question": "Drop bảng fact_events",
            "status": "blocked",
            "intent": "unsupported",
            "recent_context": [],
        }
    )
    assert len(chips) >= 3
    joined = " ".join(item["question"].lower() for item in chips)
    assert "drop" not in joined and "delete" not in joined
    assert "fact_events" in joined


# --- Response -> state mapping ----------------------------------------------

def test_response_to_state_success_mapping():
    response = {
        "status": "success",
        "generated_sql": "SELECT brand, revenue FROM iceberg.gold.daily_brand_summary LIMIT 10",
        "answer": "Brand đứng đầu là apple ...",
        "rows": [{"brand": "apple", "revenue": 100}],
        "insights": ["x"],
        "chart_type": "bar",
        "chart": {"recommended": True, "type": "bar", "x": "brand", "y": "revenue", "series": None},
        "chart_data": [{"x": "apple", "y": 100}],
        "retry_count": 0,
        "model_used": "llama-3.3-70b-versatile",
        "intent": "ranking",
        "used_tables": ["iceberg.gold.daily_brand_summary"],
        "answer_type": "answer",
        "needs_clarification": False,
        "clarification_suggestions": [],
        "assumptions": [],
        "error_message": None,
    }
    state = runner.response_to_state(response)
    assert state["agent_engine"] == "v2"
    assert state["status"] == "success"
    assert state["guard_status"] == "pass"
    assert state["summary"] == response["answer"]
    assert state["query_result"] == response["rows"]
    assert state["chart_suggestion"] == {
        "chart_type": "bar", "x": "brand", "y": "revenue", "series": None, "sort": None,
    }
    assert state["agent_trace"]["intent"] == "ranking"
    assert state["agent_trace"]["answer_type"] == "answer"


def test_response_to_state_error_and_blocked_mapping():
    err = runner.response_to_state({"status": "error", "error_message": "boom", "rows": []})
    assert err["status"] == "failed"
    assert err["guard_status"] == "error"

    blocked = runner.response_to_state({"status": "blocked", "error_message": "nope", "rows": []})
    assert blocked["status"] == "blocked"
    assert blocked["guard_status"] == "blocked"


# --- Conversational assistant (free-form prompts) ---------------------------

def test_conversation_falls_back_without_key(monkeypatch):
    # No Groq key -> must not call out, must signal llm_used=False, never raise.
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    out = conversation.answer_conversational("Giải thích fact_events khác fact_sales thế nào?", [])
    assert out["llm_used"] is False
    assert out["answer"] == ""
    assert out["follow_ups"] == []


def test_build_overview_text_renders_business_semantics():
    overview = {
        "source": "catalog",
        "tables": [
            {
                "table_name": "daily_event_summary",
                "display_name": "Daily event summary",
                "purpose": "Daily traffic, conversion, revenue.",
                "grain": "1 row = 1 event_date.",
                "use_for": "Daily totals.",
                "columns": [
                    {"name": "event_date", "type": "date", "meaning": "ngày", "business_terms": ""},
                    {"name": "total_revenue", "type": "double", "meaning": "doanh thu", "business_terms": "revenue"},
                ],
            }
        ],
    }
    text = conversation.build_overview_text(overview)
    assert f"{GOLD_PREFIX}.daily_event_summary" in text
    assert "Daily traffic, conversion, revenue." in text
    assert "total_revenue" in text


def test_answer_type_for_conversational_answer_is_answer():
    answered = {"intent": "unsupported", "status": "success", "conversational_answer": True, "row_count": 0}
    assert suggestions.answer_type_for(answered) == "answer"

    unanswered = {"intent": "unsupported", "status": "success", "conversational_answer": False, "row_count": 0}
    assert suggestions.answer_type_for(unanswered) == "clarification"


def test_assistive_clarification_node_falls_back_without_key(monkeypatch):
    pytest.importorskip("langgraph")
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    from agent.engine_v2 import graph

    out = graph.assistive_clarification_node(
        {"question": "Bạn giúp được gì?", "effective_question": "Bạn giúp được gì?", "recent_context": []}
    )
    assert out["conversational_answer"] is False
    assert out["answer_type"] == "clarification"
    assert out["answer"]  # a non-empty deterministic fallback
    assert out["conversational_suggestions"] == []
    assert out["status"] == "success"


def test_assistive_clarification_node_uses_conversational_answer(monkeypatch):
    pytest.importorskip("langgraph")
    from agent.engine_v2 import graph

    monkeypatch.setattr(
        conversation,
        "answer_conversational",
        lambda *args, **kwargs: {
            "answer": "fact_events chứa mọi event, fact_sales chỉ chứa purchase.",
            "follow_ups": ["Doanh thu theo ngày trong tháng 1/2020 là bao nhiêu?"],
            "llm_used": True,
            "error": None,
        },
    )

    out = graph.assistive_clarification_node(
        {
            "question": "fact_events khác fact_sales thế nào?",
            "effective_question": "fact_events khác fact_sales thế nào?",
            "recent_context": [],
        }
    )
    assert out["conversational_answer"] is True
    assert out["answer_type"] == "answer"
    assert "fact_sales" in out["answer"]
    assert len(out["conversational_suggestions"]) == 1
    assert out["conversational_suggestions"][0]["question"].startswith("Doanh thu theo ngày")


# --- Conversation context: LLM follow-up rewriting --------------------------

_PREVIOUS_RANKING_SPEC = {
    "intent": "ranking",
    "dimension": "brand",
    "metric": "total_views",
    "analysis_type": "topk",
    "time_range": {"type": "date_range", "field": "event_date", "start": "2020-01-01", "end": "2020-12-31"},
    "time_grain": None,
    "filters": [],
    "comparison_entities": [],
    "sort_direction": "desc",
    "limit": 10,
    "table_candidates": ["daily_brand_summary"],
    "extracted_entities": {"metrics": ["total_views"]},
    "nlu_confidence": "high",
}

_PREVIOUS_RANKING_TURN = {
    "turn_id": 1,
    "status": "success",
    "intent": "ranking",
    "question": "Nhãn hàng nào có số lượt xem nhiều nhất trong 2020",
    "generated_sql": (
        "SELECT brand, view_count FROM iceberg.gold.daily_brand_summary "
        "ORDER BY view_count DESC LIMIT 10"
    ),
    "answer": "Nhãn hàng đứng đầu là ...",
    "effective_spec": _PREVIOUS_RANKING_SPEC,
    "result_columns": ["brand", "view_count"],
    "result_sample": [
        {"brand": "apple", "view_count": 100},
        {"brand": "samsung", "view_count": 80},
        {"brand": "unknown", "view_count": 70},
        {"brand": "sony", "view_count": 60},
    ],
}


def test_llm_rewrite_followup_no_key_is_not_followup(monkeypatch):
    # Without a Groq key the rewriter must no-op (echo the question), never raise.
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    out = context.llm_rewrite_followup("Bỏ qua nhãn hàng unknown", [_PREVIOUS_RANKING_TURN])
    assert out["is_followup"] is False
    assert out["standalone_question"] == "Bỏ qua nhãn hàng unknown"


def test_llm_rewrite_followup_no_context_is_not_followup(monkeypatch):
    monkeypatch.setattr(context.llm, "llm_available", lambda: True)
    out = context.llm_rewrite_followup("Bỏ qua nhãn hàng unknown", [])
    assert out["is_followup"] is False


def test_llm_rewrite_followup_merges_previous_turn(monkeypatch):
    monkeypatch.setattr(context.llm, "llm_available", lambda: True)
    standalone = (
        "Trong năm 2020, nhãn hàng nào có số lượt xem nhiều nhất, "
        "loại trừ nhãn hàng unknown? Trả về nhãn hàng và số lượt xem."
    )

    captured = {}

    def fake_chat_completion(messages, **kwargs):
        captured["messages"] = messages
        import json as _json

        return _json.dumps(
            {"is_followup": True, "standalone_question": standalone, "reason": "merged exclusion"}
        )

    monkeypatch.setattr(context.llm, "chat_completion", fake_chat_completion)
    out = context.llm_rewrite_followup("Bỏ qua nhãn hàng unknown", [_PREVIOUS_RANKING_TURN])

    assert out["is_followup"] is True
    assert out["standalone_question"] == standalone
    assert "unknown" in out["standalone_question"].lower()
    # It captured the previous successful turn so SQL gen can refine it.
    assert out["previous_sql"].startswith("SELECT brand, view_count")
    assert out["previous_turn_id"] == 1
    # The prior SQL was passed to the model as grounding.
    user_msg = captured["messages"][-1]["content"]
    assert "daily_brand_summary" in user_msg


def test_generate_sql_prefer_llm_skips_deterministic_and_refines(monkeypatch):
    # An intent_result that WOULD yield deterministic ranking SQL...
    intent_result = {
        "intent": "ranking",
        "dimension": "brand",
        "metric": "total_views",
        "sort_direction": "desc",
        "limit": 10,
        "table_candidates": ["daily_brand_summary"],
        "time_range": None,
        "analysis_type": "topk",
    }
    metadata_context = {
        "tables": ["daily_brand_summary"],
        "columns": {"daily_brand_summary": [{"name": "brand", "type": "varchar"}, {"name": "view_count", "type": "bigint"}]},
    }
    refined = (
        "SELECT brand, view_count FROM iceberg.gold.daily_brand_summary "
        "WHERE brand <> 'unknown' ORDER BY view_count DESC LIMIT 10"
    )
    captured = {}

    monkeypatch.setattr(sql_generator.llm, "llm_available", lambda: True)

    def fake_chat_completion(messages, **kwargs):
        captured["messages"] = messages
        return refined

    monkeypatch.setattr(sql_generator.llm, "chat_completion", fake_chat_completion)

    previous_sql = "SELECT brand, view_count FROM iceberg.gold.daily_brand_summary ORDER BY view_count DESC LIMIT 10"
    out = sql_generator.generate_sql(
        "Trong năm 2020, nhãn hàng nào có nhiều view nhất, loại trừ unknown?",
        intent_result=intent_result,
        metadata_context=metadata_context,
        prefer_llm=True,
        previous_sql=previous_sql,
    )
    assert out == refined  # used the LLM, not the deterministic template
    # The previous SQL was provided to the model for refinement.
    assert any("previous related query" in m["content"].lower() for m in captured["messages"])


def test_generate_sql_prefer_llm_falls_back_to_deterministic_without_key(monkeypatch):
    monkeypatch.setattr(sql_generator.llm, "llm_available", lambda: False)
    intent_result = {
        "intent": "ranking",
        "dimension": "brand",
        "metric": "total_views",
        "sort_direction": "desc",
        "limit": 5,
        "table_candidates": ["daily_brand_summary"],
        "time_range": None,
    }
    metadata_context = {
        "tables": ["daily_brand_summary"],
        "columns": {"daily_brand_summary": [{"name": "brand", "type": "varchar"}, {"name": "view_count", "type": "bigint"}]},
    }
    out = sql_generator.generate_sql(
        "nhãn hàng nào nhiều view nhất",
        intent_result=intent_result,
        metadata_context=metadata_context,
        prefer_llm=True,
    )
    # Degrades gracefully to a deterministic ranking query (no LLM available).
    assert out.lower().startswith("select")
    assert "daily_brand_summary" in out.lower()


def test_route_after_intent_followup_forces_sql_path():
    pytest.importorskip("langgraph")
    from agent.engine_v2 import graph

    assert graph.route_after_intent({"intent": "unsupported", "context_sql_followup": True}) == "metadata"
    assert graph.route_after_intent({"intent": "unsupported"}) == "assistive_clarification"


def test_resolve_followup_node_uses_llm_rewrite(monkeypatch):
    # An elliptical follow-up with no detectable structured delta (op=ambiguous)
    # falls through to the LLM rewriter.
    pytest.importorskip("langgraph")
    from agent.engine_v2 import graph

    standalone = "Trong năm 2020, nhãn hàng nào có doanh thu cao nhất? Trả về nhãn hàng và doanh thu."
    monkeypatch.setattr(
        graph,
        "llm_rewrite_followup",
        lambda question, recent_context: {
            "is_followup": True,
            "standalone_question": standalone,
            "reason": "rewrote an elliptical follow-up using the previous ranking",
            "previous_sql": _PREVIOUS_RANKING_TURN["generated_sql"],
            "previous_turn_id": 1,
            "previous_question": _PREVIOUS_RANKING_TURN["question"],
        },
    )

    out = graph.resolve_followup_node(
        {
            "question": "Phân tích kỹ hơn giúp tôi",
            "recent_context": [_PREVIOUS_RANKING_TURN],
            "context_notes": [],
        }
    )
    assert out["context_used"] is True
    assert out["context_sql_followup"] is True
    assert out["effective_question"] == standalone
    assert out["previous_sql"].startswith("SELECT brand, view_count")
    assert out["previous_question"] == _PREVIOUS_RANKING_TURN["question"]


def test_load_context_prefers_incoming_durable_history(monkeypatch):
    # When the service hydrates recent_context from query_runs, the graph must use
    # it and NOT fall back to the volatile process store.
    pytest.importorskip("langgraph")
    from agent.engine_v2 import graph

    def _boom(*args, **kwargs):  # the process store must not be consulted
        raise AssertionError("get_recent_context should not be called")

    monkeypatch.setattr(graph, "get_recent_context", _boom)
    out = graph.load_context_node(
        {"session_id": "s1", "recent_context": [_PREVIOUS_RANKING_TURN]}
    )
    assert out["recent_context"] == [_PREVIOUS_RANKING_TURN]


def test_load_context_falls_back_to_store_when_empty(monkeypatch):
    pytest.importorskip("langgraph")
    from agent.engine_v2 import graph

    sentinel = [{"turn_id": 9, "status": "success", "question": "from store"}]
    monkeypatch.setattr(graph, "get_recent_context", lambda *a, **k: sentinel)
    out = graph.load_context_node({"session_id": "s1", "recent_context": []})
    assert out["recent_context"] == sentinel


def test_initialize_preserves_incoming_recent_context():
    pytest.importorskip("langgraph")
    from agent.engine_v2 import graph

    out = graph.initialize_node(
        {"question": "Bỏ qua nhãn hàng unknown", "recent_context": [_PREVIOUS_RANKING_TURN]}
    )
    assert out["recent_context"] == [_PREVIOUS_RANKING_TURN]


# --- Deterministic exclusion / year / context carryover (no LLM) ------------
#
# These prove the exact reported scenario works WITHOUT a Groq key:
#   1) "Nhan hang nao co so luot xem nhieu nhat trong 2020"
#   2) "Bo qua nhan hang unknown"
# turn 2 must re-run turn 1 with a NOT IN ('unknown') filter.

_BRAND_SUMMARY_METADATA = {
    "tables": ["daily_brand_summary"],
    "columns": {
        "daily_brand_summary": [
            {"name": "event_date", "type": "date"},
            {"name": "brand", "type": "varchar"},
            {"name": "view_count", "type": "bigint"},
        ]
    },
}


def test_nlu_extract_exclusion_filters_brand():
    out = nlu.extract_exclusion_filters("bỏ qua nhãn hàng unknown")
    assert out == [{"field": "brand", "operator": "not_in", "values": ["unknown"]}]
    # ASCII phrasing the user actually typed.
    out_ascii = nlu.extract_exclusion_filters("bo qua nhan hang unknown")
    assert out_ascii == [{"field": "brand", "operator": "not_in", "values": ["unknown"]}]
    # Multiple values.
    multi = nlu.extract_exclusion_filters("loại trừ apple, sony")
    assert multi[0]["operator"] == "not_in"
    assert set(multi[0]["values"]) == {"apple", "sony"}


def test_nlu_no_exclusion_when_absent():
    assert nlu.extract_exclusion_filters("nhãn hàng nào nhiều view nhất") == []


def test_nlu_year_range_extraction():
    result = nlu.parse_nlu("Doanh thu trong 2020")
    assert result["time_range"]["type"] == "date_range"
    assert result["time_range"]["start"] == "2020-01-01"
    assert result["time_range"]["end"] == "2020-12-31"


def test_nlu_first_question_full_parse():
    result = nlu.parse_nlu("Nhan hang nao co so luot xem nhieu nhat trong 2020")
    assert result["intent"] == "ranking"
    assert result["dimension"] == "brand"
    assert result["metric"] == "total_views"
    assert result["sort_direction"] == "desc"
    assert result["time_range"]["start"] == "2020-01-01"
    assert result["extracted_entities"]["filters"] == []


def test_nlu_merged_followup_carries_exclusion():
    merged = "Nhan hang nao co so luot xem nhieu nhat trong 2020 Bo qua nhan hang unknown"
    result = nlu.parse_nlu(merged)
    assert result["intent"] == "ranking"
    assert result["dimension"] == "brand"
    assert result["metric"] == "total_views"
    assert result["time_range"]["start"] == "2020-01-01"
    assert result["filters"] == [{"field": "brand", "operator": "not_in", "values": ["unknown"]}]


def test_deterministic_ranking_aggregates_with_year_filter():
    intent_result = nlu.parse_nlu("Nhan hang nao co so luot xem nhieu nhat trong 2020")
    sql = sql_generator.generate_sql(
        "Nhan hang nao co so luot xem nhieu nhat trong 2020",
        intent_result=intent_result,
        metadata_context=_BRAND_SUMMARY_METADATA,
    )
    lowered = sql.lower()
    assert "sum(view_count) as view_count" in lowered
    assert "group by brand" in lowered
    assert "event_date between date '2020-01-01' and date '2020-12-31'" in lowered
    assert "not in" not in lowered  # no exclusion on the first turn
    # Read-only + Gold-only: the guard accepts it.
    assert guard.validate_sql(sql).lower().startswith("select")


def test_deterministic_ranking_applies_exclusion_filter():
    merged = "Nhan hang nao co so luot xem nhieu nhat trong 2020 Bo qua nhan hang unknown"
    intent_result = nlu.parse_nlu(merged)
    sql = sql_generator.generate_sql(merged, intent_result=intent_result, metadata_context=_BRAND_SUMMARY_METADATA)
    lowered = sql.lower()
    assert "lower(brand) not in ('unknown')" in lowered
    assert "sum(view_count) as view_count" in lowered
    assert "group by brand" in lowered
    assert "event_date between date '2020-01-01' and date '2020-12-31'" in lowered
    assert guard.validate_sql(sql).lower().startswith("select")


def test_exclusion_filter_skipped_when_column_absent():
    # daily_event_summary has no brand column; the brand exclusion must be dropped
    # rather than emitting invalid SQL.
    event_metadata = {
        "tables": ["daily_event_summary"],
        "columns": {"daily_event_summary": [{"name": "event_date", "type": "date"}, {"name": "total_views", "type": "bigint"}]},
    }
    cond = sql_generator._filter_conditions(
        [{"field": "brand", "operator": "not_in", "values": ["unknown"]}],
        "daily_event_summary",
        event_metadata,
    )
    assert cond is None


def test_resolve_followup_exclusion_carries_previous_question():
    # The deterministic resolver (no LLM) merges the previous question with the
    # exclusion message so it re-runs with context.
    out = context.resolve_followup("Bo qua nhan hang unknown", [_PREVIOUS_RANKING_TURN])
    assert out["action"] == "resolved_question"
    assert out["context_used"] is True
    resolved = out["resolved_question"]
    assert _PREVIOUS_RANKING_TURN["question"] in resolved
    assert "bo qua nhan hang unknown" in resolved.lower()


def test_resolve_followup_exclusion_then_deterministic_sql_end_to_end():
    # Full deterministic chain: resolver -> NLU -> SQL, mirroring the graph.
    resolved = context.resolve_followup("Bo qua nhan hang unknown", [_PREVIOUS_RANKING_TURN])
    effective_question = resolved["resolved_question"]
    intent_result = nlu.parse_nlu(effective_question)
    sql = sql_generator.generate_sql(
        effective_question, intent_result=intent_result, metadata_context=_BRAND_SUMMARY_METADATA
    ).lower()
    assert "lower(brand) not in ('unknown')" in sql
    assert "group by brand" in sql


# --- spec merge engine ------------------------------------------------------

def test_merge_spec_exclusion_accumulates_then_includes():
    spec = dict(_PREVIOUS_RANKING_SPEC)
    spec = spec_mod.merge_spec(spec, {"add_filters": [{"field": "brand", "operator": "not_in", "values": ["unknown"]}]})
    spec = spec_mod.merge_spec(spec, {"add_filters": [{"field": "brand", "operator": "not_in", "values": ["sony"]}]})
    not_in = [f for f in spec["filters"] if f["operator"] == "not_in"][0]
    assert set(not_in["values"]) == {"unknown", "sony"}  # exclusions accumulate


def test_merge_spec_metric_switch_resets_metric_list_and_table():
    spec = spec_mod.merge_spec(dict(_PREVIOUS_RANKING_SPEC), {"set": {"metric": "revenue"}})
    assert spec["metric"] == "revenue"
    assert spec["extracted_entities"]["metrics"] == ["revenue"]


def test_merge_spec_dimension_switch_recomputes_table():
    spec = spec_mod.merge_spec(dict(_PREVIOUS_RANKING_SPEC), {"set": {"dimension": "category"}})
    assert spec["dimension"] == "category"
    assert spec["table_candidates"] == ["daily_category_summary"]


def test_merge_spec_remove_filter():
    spec = dict(_PREVIOUS_RANKING_SPEC)
    spec = spec_mod.merge_spec(spec, {"add_filters": [{"field": "brand", "operator": "not_in", "values": ["unknown"]}]})
    spec = spec_mod.merge_spec(spec, {"remove_filter_fields": ["brand"]})
    assert all(f.get("field") != "brand" for f in spec["filters"])


# --- classify_followup ops --------------------------------------------------

_RICH_BRAND_MD = {
    "tables": ["daily_brand_summary"],
    "columns": {
        "daily_brand_summary": [
            {"name": "event_date", "type": "date"},
            {"name": "brand", "type": "varchar"},
            {"name": "view_count", "type": "bigint"},
            {"name": "purchase_count", "type": "bigint"},
            {"name": "revenue", "type": "double"},
        ]
    },
}


def _ctx(*turns):
    """recent_context is newest-first."""
    return list(turns)


def test_classify_reset_and_meta_and_new_query():
    assert context.classify_followup("thôi chuyển chủ đề khác", [_PREVIOUS_RANKING_TURN])["op"] == "reset"
    assert context.classify_followup("bạn làm được gì?", [_PREVIOUS_RANKING_TURN])["op"] == "meta"
    # A complete standalone question is a brand-new query, not a refine.
    out = context.classify_followup("Doanh thu theo category trong 2021 là bao nhiêu?", [_PREVIOUS_RANKING_TURN])
    assert out["op"] == "new_query"


def test_classify_refine_exclusion_is_not_in():
    out = context.classify_followup("Bỏ qua nhãn hàng unknown", [_PREVIOUS_RANKING_TURN])
    assert out["op"] == "refine"
    filters = out["merged_spec"]["filters"]
    assert any(f["operator"] == "not_in" and "unknown" in f["values"] for f in filters)


def test_classify_entity_ref_remaining_excludes_leader():
    out = context.classify_followup("cho tôi những cái còn lại", [_PREVIOUS_RANKING_TURN])
    assert out["op"] == "entity_ref"
    filters = out["merged_spec"]["filters"]
    assert any(f["operator"] == "not_in" and "apple" in f["values"] for f in filters)


def test_classify_entity_ref_named_focus():
    out = context.classify_followup("chi tiết của samsung", [_PREVIOUS_RANKING_TURN])
    assert out["op"] == "entity_ref"
    filters = out["merged_spec"]["filters"]
    assert any(f["operator"] == "in" and "samsung" in f["values"] for f in filters)


def test_scenario_compounding_refinements_end_to_end():
    """The headline goal: a chain of follow-ups compounds correctly."""
    md = _RICH_BRAND_MD

    # T1 — new analytical question.
    q1 = "Nhan hang nao co so luot xem nhieu nhat trong 2020"
    spec1 = spec_mod.canonical_spec(nlu.parse_nlu(q1))
    sql1 = sql_generator.generate_sql(q1, intent_result=spec1, metadata_context=md).lower()
    assert "sum(view_count)" in sql1 and "2020-01-01" in sql1
    turn1 = {"turn_id": 1, "status": "success", "intent": spec1["intent"], "question": q1,
             "generated_sql": sql1, "effective_spec": spec1,
             "result_columns": ["brand", "view_count"],
             "result_sample": [{"brand": "apple", "view_count": 100}, {"brand": "unknown", "view_count": 70}]}

    def refine(message, ctx):
        cls = context.classify_followup(message, ctx)
        assert cls["op"] in ("refine", "entity_ref"), (message, cls["op"])
        spec = cls["merged_spec"]
        sql = sql_generator.generate_sql(
            spec_mod.render_spec_question(spec), intent_result=spec, metadata_context=md
        ).lower()
        turn = {"turn_id": 0, "status": "success", "intent": spec["intent"], "question": message,
                "generated_sql": sql, "effective_spec": spec,
                "result_columns": ["brand", "view_count"], "result_sample": turn1["result_sample"]}
        return spec, sql, turn

    # T2 — exclude unknown.
    _s2, sql2, turn2 = refine("Bỏ qua nhãn hàng unknown", _ctx(turn1))
    assert "lower(brand) not in ('unknown')" in sql2 and "sum(view_count)" in sql2

    # T3 — top 5 only (exclusion must persist).
    _s3, sql3, turn3 = refine("top 5 thôi", _ctx(turn2, turn1))
    assert "limit 5" in sql3 and "not in ('unknown')" in sql3

    # T4 — switch metric to revenue (exclusion + limit persist).
    _s4, sql4, turn4 = refine("đổi sang doanh thu", _ctx(turn3, turn2, turn1))
    assert "sum(revenue)" in sql4 and "not in ('unknown')" in sql4 and "limit 5" in sql4

    # T5 — change the year to 2021 (everything else persists).
    _s5, sql5, _turn5 = refine("còn 2021 thì sao", _ctx(turn4, turn3, turn2, turn1))
    assert "2021-01-01" in sql5 and "sum(revenue)" in sql5 and "not in ('unknown')" in sql5 and "limit 5" in sql5
    # And every compounded query stays read-only + Gold-only.
    assert guard.validate_sql(sql5).lower().startswith("select")


def test_scenario_threshold_and_inclusion():
    md = _RICH_BRAND_MD
    spec1 = spec_mod.canonical_spec(nlu.parse_nlu("top brand theo view 2020"))
    turn1 = {"turn_id": 1, "status": "success", "intent": "ranking",
             "question": "top brand theo view 2020", "generated_sql": "x", "effective_spec": spec1,
             "result_columns": ["brand", "view_count"], "result_sample": [{"brand": "apple", "view_count": 100}]}

    # Threshold -> HAVING on the aggregate.
    out = context.classify_followup("chỉ giữ brand trên 1000 view", _ctx(turn1))
    assert out["op"] == "refine"
    sql = sql_generator.generate_sql(
        spec_mod.render_spec_question(out["merged_spec"]), intent_result=out["merged_spec"], metadata_context=md
    ).lower()
    assert "having sum(view_count) > 1000" in sql


def test_llm_extract_patch_no_key_returns_none(monkeypatch):
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    assert context.llm_extract_patch("phân tích kỹ hơn", [_PREVIOUS_RANKING_TURN]) is None


def test_llm_extract_patch_sanitizes(monkeypatch):
    monkeypatch.setattr(context.llm, "llm_available", lambda: True)
    import json as _json

    monkeypatch.setattr(
        context.llm,
        "chat_completion",
        lambda *a, **k: _json.dumps(
            {"is_followup": True, "patch": {"set": {"limit": 3, "dimension": "category", "bogus": "x"},
                                            "add_filters": [{"field": "brand", "operator": "not_in", "values": ["unknown"]}]}}
        ),
    )
    patch = context.llm_extract_patch(" ", [_PREVIOUS_RANKING_TURN])
    assert patch["set"]["limit"] == 3
    assert patch["set"]["dimension"] == "category"
    assert "bogus" not in patch["set"]  # unknown field dropped
    assert patch["add_filters"][0]["operator"] == "not_in"
