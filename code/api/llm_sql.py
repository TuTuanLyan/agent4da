import os
import re

from dotenv import load_dotenv
from groq import Groq

load_dotenv()

DEFAULT_MODEL = "llama-3.3-70b-versatile"

SYSTEM_PROMPT = """You are a Trino SQL generator. Return SQL only. No markdown. No explanation.
Only generate SELECT queries.
Use this table:
postgresql.analytics_test.test_sales(event_date, brand, category, total_purchases, total_revenue)

Rules:
- Use Trino SQL.
- Never generate INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE, CREATE, MERGE, CALL, GRANT, REVOKE.
- Use LIMIT 20 for detail queries.
- For revenue questions, use total_revenue.
- For purchase questions, use total_purchases."""


def _clean_sql_response(sql: str) -> str:
    sql = sql.strip()
    fence_match = re.fullmatch(r"```(?:sql)?\s*(.*?)\s*```", sql, flags=re.IGNORECASE | re.DOTALL)
    if fence_match:
        sql = fence_match.group(1)

    sql = sql.strip()
    if sql.endswith(";"):
        sql = sql[:-1].rstrip()
    return sql


def generate_sql(question: str) -> str:
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key or api_key == "your_groq_api_key_here":
        raise ValueError("GROQ_API_KEY is not set")

    client = Groq(api_key=api_key)
    completion = client.chat.completions.create(
        model=os.getenv("GROQ_MODEL", DEFAULT_MODEL),
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Question: {question}"},
        ],
        temperature=0,
        max_completion_tokens=200,
    )

    content = completion.choices[0].message.content or ""
    sql = _clean_sql_response(content)
    if not sql:
        raise ValueError("Groq returned an empty SQL response")
    return sql
