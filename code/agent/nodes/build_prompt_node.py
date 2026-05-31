import json


SYSTEM_PROMPT = """
You are a Trino SQL expert for an ecommerce Gold semantic layer.

Rules:
- Generate only Trino SQL.
- Use only the provided tables and columns.
- Use table names exactly as shown in the schema context.
- Use column names exactly as listed under the chosen table.
- Column names are not interchangeable across tables.
- Generate SELECT only.
- Do not use INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE, or CREATE.
- Do not answer directly in natural language.
- Prefer Gold summary tables when they already answer the question.
- Map Vietnamese business terms using each column's meaning and business_terms.
- If using gold.daily_event_summary for revenue, use total_revenue.
- Respect the grain requested by the user.
- If the question needs more detail than a summary table has, use a visible detail table or a safe join shown by the metadata.
- If the user asks for top brands, the final result must contain one row per brand.
- Do not add extra GROUP BY dimensions unless the user explicitly asks for ranking by those dimensions.
- If the user asks for top brand plus its top category/product, first aggregate total metric by brand, then use a CTE with ROW_NUMBER() OVER (PARTITION BY brand ORDER BY metric DESC) to select the top related category/product per brand.
- Treat brand value 'unknown' as missing/unknown brand, not a real brand. Display it as 'Không rõ' or exclude it when the user asks for real/notable brands.
- If the user question is a follow-up, use APP CONTEXT to resolve omitted metric, dimension, filters, time range, and previous SQL intent.
- If the user asks to draw or change a chart only, re-run a SQL query that returns the same analytical data from the previous context.
- If the user asks to exclude a brand/category/product mentioned in the follow-up, add the exclusion filter to the new SQL.
- When the question asks for product information, return product_id because this Gold layer does not have product_name.
- Add a LIMIT when returning ranking/detail rows unless the user asks for all rows.
- Return SQL only. Do not explain. Do not wrap the SQL in markdown.
""".strip()


def build_retry_context(state):
    last_error = state.get("last_sql_error")
    last_sql = state.get("generated_sql")

    if not last_error:
        return ""

    return f"""
PREVIOUS SQL FAILED:
{last_sql or ""}

TRINO ERROR:
{last_error}

Fix the SQL using the same schema context. Return only the corrected SQL.
""".strip()


def build_app_context(state):
    app_context = state.get("app_context") or {}
    if not app_context:
        return "No previous app context."

    compact_context = {
        "conversation_summary": app_context.get("conversation_summary") or "",
        "last_question": app_context.get("last_question") or "",
        "last_sql": app_context.get("last_sql") or "",
        "last_result_columns": app_context.get("last_result_columns") or [],
        "last_result_sample": app_context.get("last_result_sample") or [],
        "last_chart_suggestion": app_context.get("last_chart_suggestion") or {},
        "last_answer_kind": app_context.get("last_answer_kind") or "",
    }
    return json.dumps(compact_context, ensure_ascii=False, default=str, indent=2)


def build_requery_context(state):
    validation = state.get("result_validation") or {}
    if not validation.get("can_requery"):
        return ""

    return f"""
PREVIOUS SQL RAN BUT RESULT WAS INCOMPLETE:
{state.get("generated_sql") or ""}

RESULT VALIDATION:
{json.dumps(validation, ensure_ascii=False, default=str, indent=2)}

Regenerate SQL using the same schema context and app context. Include the missing fields when they exist in the Gold metadata. Return only corrected SQL.
""".strip()


def build_prompt_node(state):
    retry_context = build_retry_context(state)
    requery_context = build_requery_context(state)
    prompt = f"""
{SYSTEM_PROMPT}

SCHEMA CONTEXT:
{state["schema_context"]}

APP CONTEXT:
{build_app_context(state)}

USER QUESTION:
{state["user_question"]}

{retry_context}

{requery_context}
""".strip()

    return {
        "prompt": prompt,
    }
