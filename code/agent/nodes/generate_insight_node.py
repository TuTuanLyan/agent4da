import json
import re

from services.llm_service import generate_text


MAX_INSIGHT_ROWS = 20


def to_json_text(value):
    return json.dumps(value, ensure_ascii=False, default=str, indent=2)


def build_insight_prompt(state, rows_preview, chart_spec):
    return f"""
Bạn là trợ lý phân tích dữ liệu cho một hệ thống Text-to-SQL.

Câu hỏi người dùng:
{state.get("user_question") or ""}

SQL đã thực thi:
{state.get("generated_sql") or ""}

Chart spec:
{to_json_text(chart_spec)}

Preview query_result, tối đa {MAX_INSIGHT_ROWS} dòng:
{to_json_text(rows_preview)}

Yêu cầu:
- Chỉ dùng số liệu có trong query_result ở trên.
- Không bịa số, không suy diễn ngoài dữ liệu.
- Nếu kết quả thiếu cột, thiếu chiều phân tích, thiếu khoảng thời gian, hoặc quá tổng hợp so với câu hỏi, ghi rõ phần thiếu trong missing_info.
- Nếu không thiếu thông tin, missing_info.has_missing_info = false và missing_info.items = [].
- Xem brand = 'unknown' là "Không rõ" / missing brand, không phải một nhãn hàng thật.
- Không mô tả unknown như một nhãn hàng bình thường.
- Nếu cùng một brand xuất hiện nhiều lần, nói rõ kết quả đang group theo chiều bổ sung và có thể không phải ranking brand duy nhất.
- Không gộp các brand trùng kiểu "Apple (440 và 170)" trừ khi SQL cố ý trả nhiều dòng cho cùng brand.
- Nếu không có dữ liệu thì nói không có dữ liệu phù hợp.
- Viết bằng tiếng Việt, ngắn gọn, dễ hiểu.
- Trả về JSON hợp lệ duy nhất, không markdown, theo schema:
{{
  "insight_summary": "3-5 câu tiếng Việt",
  "missing_info": {{
    "has_missing_info": false,
    "items": [],
    "can_requery": false,
    "notes": ""
  }}
}}
""".strip()


def default_missing_info(has_missing_info=False, items=None, can_requery=False, notes=""):
    return {
        "has_missing_info": has_missing_info,
        "items": items or [],
        "can_requery": can_requery,
        "notes": notes,
    }


def parse_json_object(text):
    text = (text or "").strip()
    text = re.sub(r"^\s*```(?:json)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```\s*$", "", text)

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not match:
            raise
        return json.loads(match.group(0))


def generate_insight_node(state):
    if state.get("error"):
        return {
            "insight_summary": "Không thể tạo nhận định vì truy vấn đang có lỗi. Vui lòng kiểm tra SQL hoặc kết nối dữ liệu.",
            "missing_info": default_missing_info(
                True,
                ["Truy vấn chưa chạy thành công nên chưa đủ dữ liệu để phân tích."],
                can_requery=True,
                notes=state.get("error") or "",
            ),
        }

    rows = state.get("query_result") or []
    if not rows:
        return {
            "insight_summary": "Không có dữ liệu phù hợp với yêu cầu truy vấn.",
            "missing_info": default_missing_info(
                True,
                ["Kết quả truy vấn không có dòng dữ liệu phù hợp."],
                can_requery=True,
                notes="Có thể cần nới khoảng thời gian, đổi metric hoặc kiểm tra lại metadata/bảng phù hợp.",
            ),
        }

    rows_preview = rows[:MAX_INSIGHT_ROWS]
    chart_spec = dict(state.get("chart_spec") or {})
    chart_spec["data"] = (chart_spec.get("data") or [])[:MAX_INSIGHT_ROWS]
    prompt = build_insight_prompt(state, rows_preview, chart_spec)

    try:
        response_text = generate_text(prompt)
        parsed = parse_json_object(response_text)
    except Exception as exc:
        return {
            "insight_summary": "Không thể tạo nhận định tự động ở bước này.",
            "missing_info": default_missing_info(),
            "insight_error": f"Insight generation failed: {type(exc).__name__}: {exc}",
        }

    return {
        "insight_summary": str(parsed.get("insight_summary") or "").strip(),
        "missing_info": parsed.get("missing_info") or default_missing_info(),
        "insight_error": None,
    }
