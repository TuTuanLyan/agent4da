"""End-to-end test: does the agent remember context within one session?

Drives the LIVE backend over HTTP exactly like the frontend does:
  1. log in (seed admin by default)
  2. create a chat session
  3. ask an initial question that fixes a metric + time range
  4. ask a FOLLOW-UP that omits the metric and grain ("what about February?")
  5. assert the follow-up reused the prior turn:
       - response.context_used is True
       - follow-up SQL still references the revenue measure (inherited)
       - follow-up SQL targets February (the only new thing said)

Prereqs: the app backend, Trino, Postgres and an LLM key must be running.

Run:
    cd app/backend
    python tests/test_session_memory_e2e.py
    # override target / creds if needed:
    AGENT_BACKEND_URL=http://localhost:8083 \
    AGENT_TEST_EMAIL=admin@example.com AGENT_TEST_PASSWORD=Hungdzvcl2005 \
    python tests/test_session_memory_e2e.py
"""
from __future__ import annotations

import os
import re
import sys
import uuid

import httpx

BASE = os.getenv("AGENT_BACKEND_URL", "http://localhost:8083").rstrip("/")
EMAIL = os.getenv("AGENT_TEST_EMAIL", "admin@example.com")
PASSWORD = os.getenv("AGENT_TEST_PASSWORD", "Hungdzvcl2005")

Q1 = "Doanh thu theo ngay trong thang 1 nam 2020"
# A real follow-up: no metric, no grain, no year - only "February".
Q2 = "Con thang 2 thi sao?"

REVENUE_HINTS = ("total_revenue", "revenue", "doanh_thu")
FEB_HINTS = ("'2020-02", "2020-02", "month = 2", "month=2", "month) = 2", "= 2", "february", "02-01")


def login(client: httpx.Client) -> str:
    r = client.post(f"{BASE}/auth/login", json={"email": EMAIL, "password": PASSWORD})
    if r.status_code == 200:
        return r.json()["access_token"]
    # fall back to registering a throwaway user
    email = f"memtest_{uuid.uuid4().hex[:8]}@example.com"
    r = client.post(f"{BASE}/auth/register", json={"email": email, "password": "Test1234!"})
    r.raise_for_status()
    print(f"[setup] logged in as throwaway user {email}")
    return r.json()["access_token"]


def ask(client: httpx.Client, token: str, question: str, session_id: str) -> dict:
    r = client.post(
        f"{BASE}/agent/ask",
        headers={"Authorization": f"Bearer {token}"},
        json={"question": question, "session_id": session_id, "summarize": True},
        timeout=120,
    )
    r.raise_for_status()
    return r.json()


def has_any(text: str, hints) -> bool:
    low = (text or "").lower()
    return any(h.lower() in low for h in hints)


def main() -> int:
    try:
        with httpx.Client() as client:
            token = login(client)

            sr = client.post(
                f"{BASE}/agent/sessions",
                headers={"Authorization": f"Bearer {token}"},
            )
            sr.raise_for_status()
            session_id = sr.json()["id"]
            print(f"[setup] session_id = {session_id}\n")

            print(f"[turn 1] {Q1}")
            a1 = ask(client, token, Q1, session_id)
            sql1 = a1.get("generated_sql") or ""
            print(f"         status={a1.get('status')} context_used={a1.get('context_used')}")
            print(f"         SQL: {sql1}\n")

            print(f"[turn 2 - follow-up] {Q2}")
            a2 = ask(client, token, Q2, session_id)
            sql2 = a2.get("generated_sql") or ""
            print(f"         status={a2.get('status')} context_used={a2.get('context_used')}")
            print(f"         SQL: {sql2}\n")
    except (httpx.HTTPError, KeyError) as exc:
        print(f"[ERROR] Could not reach the backend at {BASE}: {exc!r}")
        print("        Start the stack (backend + Trino + Postgres + LLM key) and retry.")
        return 2

    # --- assertions ---------------------------------------------------------
    checks = []
    checks.append(("turn 1 produced SQL", bool(sql1)))
    checks.append(("turn 2 produced SQL", bool(sql2)))
    checks.append(("turn 2 reports context_used=True", a2.get("context_used") is True))
    checks.append(("turn 2 SQL still references the revenue metric (inherited)", has_any(sql2, REVENUE_HINTS)))
    checks.append(("turn 2 SQL targets February (the new constraint)", has_any(sql2, FEB_HINTS)))

    print("=" * 64)
    passed = 0
    for label, ok in checks:
        print(f"  [{'PASS' if ok else 'FAIL'}] {label}")
        passed += int(bool(ok))
    print("=" * 64)

    verdict = passed == len(checks)
    if verdict:
        print("VERDICT: the model REMEMBERS context within the session. ✅")
    else:
        print("VERDICT: same-session memory is NOT working as expected. ❌")
        print("Hint: confirm app_context is passed into graph.invoke and that")
        print("build_prompt_node renders APP CONTEXT (not 'No previous app context.').")
    return 0 if verdict else 1


if __name__ == "__main__":
    sys.exit(main())
