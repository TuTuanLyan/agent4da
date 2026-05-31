import json
import os
import re
from typing import Any

from dotenv import load_dotenv

try:
    from groq import Groq
except ModuleNotFoundError:
    Groq = None

load_dotenv()

DEFAULT_MODEL = "llama-3.3-70b-versatile"
LLM_INSIGHT_INTENTS = {
    "metric_overview",
    "ranking",
    "revenue_sales",
    "trend",
    "drilldown",
    "comparison",
    "breakdown",
    "conversion_funnel",
}
MAX_LLM_ROWS = 10


DIMENSION_LABELS = {
    "brand": "Brand",
    "category_l1": "Category",
    "category_l2": "Category",
    "category_l3": "Category",
    "product_id": "Sản phẩm",
}

METRIC_LABELS = {
    "unique_events": "event",
    "total_events": "event",
    "view_count": "view",
    "total_views": "view",
    "cart_count": "cart",
    "total_carts": "cart",
    "purchase_count": "purchase",
    "total_purchases": "purchase",
    "revenue": "doanh thu",
    "total_revenue": "doanh thu",
    "gross_amount": "doanh thu",
}


def _is_number(value: Any) -> bool:
    return not isinstance(value, bool) and isinstance(value, (int, float))


def _first_existing(row: dict[str, Any], candidates: tuple[str, ...]) -> tuple[str, Any] | tuple[None, None]:
    for column_name in candidates:
        if column_name in row:
            return column_name, row[column_name]
    return None, None


def _first_numeric_metric(row: dict[str, Any]) -> tuple[str, Any] | tuple[None, None]:
    for column_name, value in row.items():
        normalized = column_name.lower()
        if normalized.endswith("_id") or normalized == "id":
            continue
        if _is_number(value):
            return column_name, value
    return None, None


def _revenue_value(rows: list[dict[str, Any]]) -> Any:
    if not rows:
        return None
    for column_name, value in rows[0].items():
        normalized = column_name.lower()
        if any(token in normalized for token in ("revenue", "amount", "doanh_thu", "doanhthu")):
            return value
    return None


def _with_source(result: dict[str, Any], source: str, error: str | None = None) -> dict[str, Any]:
    output = {
        **result,
        "insight_source": source,
        "llm_insight_used": source == "llm",
    }
    if error:
        output["llm_insight_error"] = error
    return output


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return str(value)


def _clean_json_response(content: str) -> str:
    content = content.strip()
    fence_match = re.fullmatch(r"```(?:json)?\s*(.*?)\s*```", content, flags=re.IGNORECASE | re.DOTALL)
    if fence_match:
        content = fence_match.group(1).strip()

    if not content.startswith("{"):
        start = content.find("{")
        end = content.rfind("}")
        if start >= 0 and end > start:
            content = content[start : end + 1]
    return content


def _warning_mentions_revenue_caveat(warnings: list[str]) -> bool:
    normalized = " ".join(warnings).lower()
    return any(token in normalized for token in ("doanh thu", "revenue", "purchase", "fact_sales"))


def _enforce_current_data_phrase(answer: str) -> str:
    if "trong dữ liệu hiện tại" in answer.lower():
        return answer
    answer = answer.rstrip()
    if answer.endswith("."):
        return f"{answer[:-1]} trong dữ liệu hiện tại."
    return f"{answer} trong dữ liệu hiện tại."


def _enforce_revenue_caveat(answer: str, insights: list[str], warnings: list[str]) -> tuple[str, list[str]]:
    if not _warning_mentions_revenue_caveat(warnings):
        return answer, insights

    combined = " ".join([answer, *insights]).lower()
    if "purchase" in combined or "doanh thu đang bằng 0" in combined or "fact_sales" in combined:
        return answer, insights

    caveat = "Lưu ý: dữ liệu hiện tại có thể không có hoặc chưa ghi nhận purchase event, nên doanh thu bằng 0."
    answer = f"{answer.rstrip()} {caveat}"
    return answer, [*insights, caveat]


def _has_preview_count_hallucination(answer: str, insights: list[str], row_count: int) -> bool:
    if row_count == MAX_LLM_ROWS:
        return False
    combined = " ".join([answer, *insights]).lower()
    return bool(re.search(rf"\b{MAX_LLM_ROWS}\s+(?:dòng|dong|rows?)\b", combined))


def _rule_based_fallback(
    *,
    question: str,
    intent: str,
    rows: list[dict[str, Any]],
    warnings: list[str],
    generated_sql: str,
    table_candidates: list[str],
    used_tables: list[str],
    error: str | None = None,
) -> dict[str, Any]:
    return _with_source(
        generate_insight(
            question=question,
            intent=intent,
            rows=rows,
            warnings=warnings,
            generated_sql=generated_sql,
            table_candidates=table_candidates,
            used_tables=used_tables,
        ),
        "rule_based",
        error=error,
    )


def _metadata_tables_answer(rows: list[dict[str, Any]]) -> dict[str, Any]:
    tables = [str(next(iter(row.values()))) for row in rows]
    summary_tables = [table for table in tables if "summary" in table]
    fact_tables = [table for table in tables if table.startswith("fact_")]
    dim_tables = [table for table in tables if table.startswith("dim_")]
    insights = []
    if fact_tables:
        insights.append(f"Fact tables: {', '.join(fact_tables)}.")
    if dim_tables:
        insights.append(f"Dimension tables: {', '.join(dim_tables)}.")
    if summary_tables:
        insights.append(f"Summary tables: {', '.join(summary_tables)}.")
    return {
        "answer": f"Hệ thống hiện có {len(tables)} bảng Gold trong dữ liệu hiện tại.",
        "insights": insights,
    }


def _metadata_columns_answer(rows: list[dict[str, Any]], table_candidates: list[str]) -> dict[str, Any]:
    table_name = table_candidates[0] if table_candidates else "bảng này"
    column_names = [str(row.get("column_name")) for row in rows if row.get("column_name")]
    preview = ", ".join(column_names[:6])
    insights = [f"Một số cột đầu: {preview}."] if preview else []
    return {
        "answer": f"Bảng {table_name} hiện có {len(rows)} cột trong dữ liệu hiện tại.",
        "insights": insights,
    }


def _metric_overview_answer(rows: list[dict[str, Any]]) -> dict[str, Any]:
    row = rows[0]
    parts = []
    if "total_events" in row:
        parts.append(f"{row['total_events']} event")
    if "total_views" in row:
        parts.append(f"{row['total_views']} lượt view")
    if "total_carts" in row:
        parts.append(f"{row['total_carts']} lượt cart")
    if "total_purchases" in row:
        parts.append(f"{row['total_purchases']} lượt purchase")

    if parts:
        return {
            "answer": f"Trong dữ liệu hiện tại có {', '.join(parts)}.",
            "insights": parts,
        }

    return {
        "answer": "Kết quả tổng quan đã được truy vấn từ dữ liệu hiện tại.",
        "insights": [f"{key} = {value}" for key, value in row.items()],
    }


def _ranking_answer(rows: list[dict[str, Any]]) -> dict[str, Any]:
    row = rows[0]
    dimension_key, dimension_value = _first_existing(
        row,
        ("brand", "category_l1", "category_l2", "category_l3", "product_id"),
    )
    metric_key, metric_value = _first_numeric_metric(row)
    if dimension_key and metric_key:
        dimension_label = DIMENSION_LABELS.get(dimension_key, dimension_key)
        metric_label = METRIC_LABELS.get(metric_key, metric_key)
        return {
            "answer": (
                f"{dimension_label} đứng đầu là {dimension_value} với "
                f"{metric_value} {metric_label} trong dữ liệu hiện tại."
            ),
            "insights": [
                f"Top result: {dimension_key}={dimension_value}, {metric_key}={metric_value}."
            ],
        }

    return {
        "answer": "Kết quả ranking đã được truy vấn từ dữ liệu hiện tại.",
        "insights": [f"{key} = {value}" for key, value in row.items()],
    }


def _revenue_answer(rows: list[dict[str, Any]], warnings: list[str]) -> dict[str, Any]:
    value = _revenue_value(rows)
    if value is None:
        return {
            "answer": "Không tìm thấy trường doanh thu rõ ràng trong kết quả hiện tại.",
            "insights": [],
        }

    has_zero_warning = any("Doanh thu đang bằng 0" in warning for warning in warnings)
    if _is_number(value) and value == 0 and has_zero_warning:
        return {
            "answer": (
                "Tổng doanh thu hiện tại là 0.0 trong dữ liệu hiện tại. "
                "Lưu ý: dữ liệu hiện tại có thể không có hoặc chưa ghi nhận purchase event, nên doanh thu bằng 0."
            ),
            "insights": ["Doanh thu trả về từ truy vấn là 0.0."],
        }

    return {
        "answer": f"Tổng doanh thu hiện tại là {value} trong dữ liệu hiện tại.",
        "insights": [f"Doanh thu = {value}."],
    }


def generate_insight(
    *,
    question: str,
    intent: str,
    rows: list[dict[str, Any]],
    warnings: list[str],
    generated_sql: str,
    table_candidates: list[str],
    used_tables: list[str],
) -> dict[str, Any]:
    if intent == "unsupported":
        return {
            "answer": "Mình cần thêm ngữ cảnh để chuyển câu hỏi này thành phân tích dữ liệu e-commerce an toàn.",
            "insights": [],
        }

    if not rows:
        return {
            "answer": "Không có dữ liệu phù hợp với câu hỏi trong dữ liệu hiện tại.",
            "insights": [],
        }

    if intent == "metadata_tables":
        return _metadata_tables_answer(rows)

    if intent == "metadata_columns":
        return _metadata_columns_answer(rows, table_candidates)

    if intent == "metric_overview":
        return _metric_overview_answer(rows)

    if intent == "ranking":
        return _ranking_answer(rows)

    if intent == "revenue_sales":
        return _revenue_answer(rows, warnings)

    if len(rows) == 1:
        row = rows[0]
        pairs = [f"{key}={value}" for key, value in list(row.items())[:4]]
        suffix = "" if len(row) <= 4 else ", ..."
        return {
            "answer": f"Kết quả trong dữ liệu hiện tại: {', '.join(pairs)}{suffix}.",
            "insights": [f"{key} = {value}" for key, value in list(row.items())[:4]],
        }

    return {
        "answer": f"Truy vấn trả về {len(rows)} dòng trong dữ liệu hiện tại.",
        "insights": [],
    }


def _build_llm_prompt(
    *,
    question: str,
    intent: str,
    rows: list[dict[str, Any]],
    row_count: int,
    generated_sql: str,
    warnings: list[str],
    chart: dict[str, Any],
    confidence: str,
    table_candidates: list[str],
    used_tables: list[str],
) -> str:
    preview_rows = [_json_safe(row) for row in rows[:MAX_LLM_ROWS]]
    payload = {
        "user_question": question,
        "intent": intent,
        "generated_sql": generated_sql,
        "row_count": row_count,
        "rows_preview_count": len(preview_rows),
        "rows_preview_limit": MAX_LLM_ROWS,
        "rows_preview_max_10": preview_rows,
        "rows_preview_note": (
            "rows_preview_max_10 contains at most 10 rows, not necessarily 10 rows. "
            "Use row_count for the actual number of returned rows. Do not infer totals "
            "beyond preview unless the SQL/result row is already an aggregate or top-k result."
        ),
        "warnings": warnings,
        "chart_recommendation": _json_safe(chart),
        "confidence": confidence,
        "table_candidates": table_candidates,
        "used_tables": used_tables,
    }
    return f"""Bạn là AI Data Analyst. Hãy diễn giải kết quả SQL thành tiếng Việt ngắn gọn.

Input JSON:
{json.dumps(payload, ensure_ascii=False, indent=2)}

Quy tắc bắt buộc:
- Chỉ dùng số liệu xuất hiện trong rows_preview_max_10 hoặc warnings.
- Không tự tạo số liệu mới.
- Không suy đoán nguyên nhân kinh doanh ngoài dữ liệu.
- Luôn dùng cụm "trong dữ liệu hiện tại".
- Nếu rows rỗng, trả lời không có dữ liệu phù hợp.
- Nếu warnings có caveat revenue/purchase, phải nhắc caveat đó.
- Với intent ranking, câu answer phải dùng cụm "đứng đầu".
- Không nói "10 dòng" chỉ vì rows preview có giới hạn tối đa 10 dòng; hãy dùng row_count.
- Không viết quá dài.
- Output JSON only, không markdown.

Output JSON:
{{
  "answer": "Một câu trả lời chính, ngắn gọn.",
  "insights": [
    "Nhận xét 1 dựa trên rows.",
    "Nhận xét 2 nếu có."
  ]
}}
"""


def _parse_llm_insight(content: str) -> dict[str, Any]:
    data = json.loads(_clean_json_response(content))
    if not isinstance(data, dict):
        raise ValueError("LLM insight response is not a JSON object")

    answer = str(data.get("answer") or "").strip()
    if not answer:
        raise ValueError("LLM insight answer is empty")

    raw_insights = data.get("insights") or []
    if not isinstance(raw_insights, list):
        raise ValueError("LLM insight insights field is not a list")

    insights = [str(item).strip() for item in raw_insights if str(item).strip()][:5]
    return {"answer": answer, "insights": insights}


def generate_llm_insight(
    *,
    question: str,
    intent: str,
    rows: list[dict[str, Any]],
    row_count: int,
    generated_sql: str,
    warnings: list[str],
    chart: dict[str, Any],
    confidence: str,
    table_candidates: list[str],
    used_tables: list[str],
) -> dict[str, Any]:
    if intent not in LLM_INSIGHT_INTENTS:
        return _rule_based_fallback(
            question=question,
            intent=intent,
            rows=rows,
            warnings=warnings,
            generated_sql=generated_sql,
            table_candidates=table_candidates,
            used_tables=used_tables,
        )

    if not rows or row_count == 0:
        return _rule_based_fallback(
            question=question,
            intent=intent,
            rows=rows,
            warnings=warnings,
            generated_sql=generated_sql,
            table_candidates=table_candidates,
            used_tables=used_tables,
        )

    api_key = os.getenv("GROQ_API_KEY")
    if Groq is None or not api_key or api_key == "your_groq_api_key_here":
        return _rule_based_fallback(
            question=question,
            intent=intent,
            rows=rows,
            warnings=warnings,
            generated_sql=generated_sql,
            table_candidates=table_candidates,
            used_tables=used_tables,
            error="LLM insight unavailable because GROQ_API_KEY is not configured.",
        )

    try:
        client = Groq(api_key=api_key, timeout=float(os.getenv("GROQ_TIMEOUT_SECONDS", "10")))
        completion = client.chat.completions.create(
            model=os.getenv("GROQ_MODEL", DEFAULT_MODEL),
            messages=[
                {
                    "role": "user",
                    "content": _build_llm_prompt(
                        question=question,
                        intent=intent,
                        rows=rows,
                        row_count=row_count,
                        generated_sql=generated_sql,
                        warnings=warnings,
                        chart=chart,
                        confidence=confidence,
                        table_candidates=table_candidates,
                        used_tables=used_tables,
                    ),
                }
            ],
            temperature=0,
            max_completion_tokens=300,
            response_format={"type": "json_object"},
        )
        content = completion.choices[0].message.content or ""
        parsed = _parse_llm_insight(content)
        parsed["answer"] = _enforce_current_data_phrase(parsed["answer"])
        parsed["answer"], parsed["insights"] = _enforce_revenue_caveat(
            parsed["answer"],
            parsed["insights"],
            warnings,
        )
        if _has_preview_count_hallucination(parsed["answer"], parsed["insights"], row_count):
            raise ValueError("LLM insight confused preview limit with actual row count")
        return _with_source(parsed, "llm")
    except Exception as exc:
        return _rule_based_fallback(
            question=question,
            intent=intent,
            rows=rows,
            warnings=warnings,
            generated_sql=generated_sql,
            table_candidates=table_candidates,
            used_tables=used_tables,
            error=str(exc),
        )
