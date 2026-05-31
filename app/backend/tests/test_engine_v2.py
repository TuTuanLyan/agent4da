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
    conversation,
    corrector,
    guard,
    insights,
    nlu,
    runner,
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
