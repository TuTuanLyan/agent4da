"""Unit test: does build_app_context_from_runs() rebuild same-session memory?

No database or network required. We feed a fake DB connection canned prior
turns and assert the compact app_context has the fields that
code/agent/nodes/build_prompt_node.build_app_context() consumes.

Run:
    cd app/backend && python -m pytest tests/test_session_memory_unit.py -v
    # or simply:
    cd app/backend && python tests/test_session_memory_unit.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# Make `import api.agent` work when run directly from app/backend.
BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api.agent import build_app_context_from_runs  # noqa: E402


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _FakeConn:
    """Mimics psycopg dict_row conn: conn.execute(sql, params).fetchall()."""

    def __init__(self, rows):
        self._rows = rows
        self.last_sql = None
        self.last_params = None

    def execute(self, sql, params=None):
        self.last_sql = sql
        self.last_params = params
        return _FakeCursor(self._rows)


SESSION_ID = "11111111-1111-1111-1111-111111111111"


def _sample_rows_newest_first():
    # query_runs is queried ORDER BY created_at DESC, so newest is first.
    return [
        {
            "question": "Doanh thu theo ngay trong thang 1 nam 2020",
            "generated_sql": (
                "SELECT event_date, total_revenue FROM gold.daily_event_summary "
                "WHERE event_date BETWEEN DATE '2020-01-01' AND DATE '2020-01-31' "
                "ORDER BY event_date"
            ),
            "columns": ["event_date", "total_revenue"],
            "rows": [
                {"event_date": "2020-01-01", "total_revenue": 1000},
                {"event_date": "2020-01-02", "total_revenue": 1200},
                {"event_date": "2020-01-03", "total_revenue": 900},
                {"event_date": "2020-01-04", "total_revenue": 1500},
            ],
            "chart_suggestion": {"chart_type": "line", "x": "event_date", "y": "total_revenue"},
            "chart_type": "line",
            "status": "success",
        },
    ]


def test_returns_empty_when_no_prior_turns():
    ctx = build_app_context_from_runs(_FakeConn([]), SESSION_ID)
    assert ctx == {}, "No prior turns should yield empty context (a fresh session has no memory)."


def test_rebuilds_compact_context_from_prior_turn():
    conn = _FakeConn(_sample_rows_newest_first())
    ctx = build_app_context_from_runs(conn, SESSION_ID)

    # Fields the SQL graph's prompt builder actually reads:
    for key in (
        "conversation_summary",
        "last_question",
        "last_sql",
        "last_result_columns",
        "last_result_sample",
        "last_chart_suggestion",
        "last_answer_kind",
    ):
        assert key in ctx, f"missing required context field: {key}"

    assert ctx["last_question"].startswith("Doanh thu theo ngay")
    assert "total_revenue" in ctx["last_sql"]
    assert ctx["last_result_columns"] == ["event_date", "total_revenue"]
    # sample capped at 3 rows
    assert len(ctx["last_result_sample"]) == 3
    assert ctx["last_chart_suggestion"]["chart_type"] == "line"
    assert "Doanh thu" in ctx["conversation_summary"]
    assert "total_revenue" in ctx["conversation_summary"]


def test_invalid_session_id_is_safe():
    ctx = build_app_context_from_runs(_FakeConn(_sample_rows_newest_first()), "not-a-uuid")
    assert ctx == {}, "A non-UUID session id must not crash; it returns empty context."


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS  {name}")
            except AssertionError as exc:
                failures += 1
                print(f"FAIL  {name}: {exc}")
    print("-" * 60)
    print("ALL PASSED" if failures == 0 else f"{failures} FAILURE(S)")
    sys.exit(1 if failures else 0)
