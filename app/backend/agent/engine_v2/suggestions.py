"""Context-aware clarification suggestions for the v2 agent.

The generator is deterministic and guardrail-neutral: it never changes the SQL
allow-list, it only proposes safer follow-up questions when the current input is
ambiguous, empty, blocked, or outside the available data sources.
"""

from __future__ import annotations

from typing import Any, Iterable, List


Suggestion = dict[str, Any]


METRIC_LABELS = {
    "revenue": "doanh thu",
    "total_revenue": "doanh thu",
    "total_events": "event",
    "total_views": "lượt xem",
    "total_carts": "lượt thêm giỏ",
    "total_purchases": "lượt mua",
    "conversion": "tỷ lệ chuyển đổi",
    "count": "số lượng",
}


def _contains_any(text: str, keywords: Iterable[str]) -> bool:
    return any(keyword in text for keyword in keywords)


def _metric_label(metric: str | None) -> str:
    return METRIC_LABELS.get(metric or "", metric or "chỉ số")


def _add(
    suggestions: List[Suggestion],
    *,
    label: str,
    question: str,
    reason: str,
    intent: str,
    confidence: str = "medium",
) -> None:
    normalized = " ".join(question.lower().split())
    if any(" ".join(str(item.get("question", "")).lower().split()) == normalized for item in suggestions):
        return
    suggestions.append(
        {
            "label": label[:64],
            "question": question,
            "reason": reason,
            "intent": intent,
            "confidence": confidence,
        }
    )


def _previous_focus(recent_context: list[dict[str, Any]]) -> tuple[str | None, str | None]:
    for turn in recent_context:
        if turn.get("status") != "success":
            continue
        intent = turn.get("intent")
        candidates = turn.get("table_candidates") or []
        question = str(turn.get("question") or "").lower()
        dimension = None
        if "daily_brand_summary" in candidates or "brand" in question:
            dimension = "brand"
        elif "daily_category_summary" in candidates or "category" in question or "danh mục" in question:
            dimension = "category"
        elif "daily_product_summary" in candidates or "product" in question or "sản phẩm" in question:
            dimension = "product"
        metric = None
        sql = str(turn.get("generated_sql") or "").lower()
        if "revenue" in sql or "doanh thu" in question:
            metric = "revenue"
        elif "view" in sql or "view" in question:
            metric = "total_views"
        elif "cart" in sql or "cart" in question:
            metric = "total_carts"
        elif "purchase" in sql or "purchase" in question:
            metric = "total_purchases"
        elif intent == "ranking":
            metric = "total_events"
        return dimension, metric
    return None, None


def build_assumptions(state: dict[str, Any]) -> list[str]:
    """Explain defaults the agent used while still answering safely."""
    intent = state.get("intent")
    assumptions: list[str] = []
    if intent in {"revenue_sales", "metric_overview", "ranking", "trend", "breakdown", "conversion_funnel"}:
        if not state.get("time_range"):
            assumptions.append("Chưa có khoảng thời gian cụ thể, nên truy vấn dùng toàn bộ dữ liệu Gold hiện có.")
    if intent == "ranking" and not state.get("dimension"):
        assumptions.append("Chưa rõ chiều xếp hạng, nên cần chọn brand, category hoặc product để phân tích chính xác hơn.")
    if intent in {"metric_overview", "revenue_sales"} and not state.get("metric"):
        assumptions.append("Chưa rõ metric cụ thể, nên cần xác nhận muốn xem doanh thu, lượt xem, cart, purchase hay conversion.")
    return assumptions


def answer_type_for(state: dict[str, Any]) -> str:
    status = state.get("status")
    intent = state.get("intent")
    if status == "blocked":
        return "blocked"
    if intent in {"metadata_tables", "metadata_columns"}:
        return "metadata"
    if intent == "unsupported":
        # A grounded free-form reply from the conversational assistant is a real
        # answer; only fall back to "clarification" when no answer was produced.
        return "answer" if state.get("conversational_answer") else "clarification"
    if status == "success" and int(state.get("row_count") or 0) == 0 and intent not in {"explain_sql", "chart_previous"}:
        return "empty_result"
    if state.get("needs_clarification"):
        return "clarification"
    return "answer"


def needs_clarification(state: dict[str, Any], assumptions: list[str], suggestions: list[Suggestion]) -> bool:
    answer_type = answer_type_for({**state, "needs_clarification": False})
    if answer_type in {"blocked", "clarification", "empty_result"}:
        return True
    if assumptions and suggestions:
        return True
    return str(state.get("nlu_confidence") or "").lower() == "low" and bool(suggestions)


def build_suggestions(state: dict[str, Any]) -> list[Suggestion]:
    question = str(state.get("question") or "")
    text = question.lower()
    intent = state.get("intent")
    metric = state.get("metric")
    dimension = state.get("dimension")
    recent_context = state.get("recent_context") or []
    previous_dimension, previous_metric = _previous_focus(recent_context)
    metric = metric or previous_metric
    metric_label = _metric_label(metric)
    suggestions: List[Suggestion] = []

    if state.get("status") == "blocked":
        _add(
            suggestions,
            label="Kiểm tra số dòng",
            question="Có bao nhiêu bản ghi trong fact_events?",
            reason="Thay thao tác thay đổi dữ liệu bằng truy vấn đọc an toàn.",
            intent="metric_overview",
            confidence="high",
        )
        _add(
            suggestions,
            label="Xem schema bảng",
            question="Bảng fact_events có những cột nào?",
            reason="Kiểm tra cấu trúc dữ liệu mà không sửa bảng.",
            intent="metadata_columns",
            confidence="high",
        )
        _add(
            suggestions,
            label="Xem dữ liệu mẫu",
            question="Liệt kê 20 event gần nhất trong fact_events",
            reason="Dùng truy vấn đọc có LIMIT để debug dữ liệu.",
            intent="drilldown",
            confidence="medium",
        )
        return suggestions

    if intent == "unsupported":
        if _contains_any(text, ("thời tiết", "thoi tiet", "weather", "crypto", "bitcoin", "tin tức", "tin tuc", "news")):
            _add(
                suggestions,
                label="Doanh thu gần nhất",
                question="Tổng doanh thu trong ngày gần nhất là bao nhiêu?",
                reason="Hệ thống hiện có dữ liệu e-commerce Gold, không có nguồn dữ liệu ngoài như thời tiết/tin tức.",
                intent="revenue_sales",
                confidence="high",
            )
            _add(
                suggestions,
                label="Xu hướng event",
                question="Số event theo ngày là bao nhiêu?",
                reason="Chuyển sang phân tích chuỗi thời gian có trong Gold data.",
                intent="trend",
                confidence="medium",
            )
            _add(
                suggestions,
                label="Top brand",
                question="Top 5 brand có nhiều event nhất trong ngày gần nhất?",
                reason="Gợi ý phân tích thương mại điện tử gần với dữ liệu hiện có.",
                intent="ranking",
                confidence="medium",
            )
        else:
            _add(
                suggestions,
                label="Chọn metric",
                question="Bạn muốn xem doanh thu, lượt xem, cart, purchase hay conversion?",
                reason="Câu hỏi chưa đủ metric để tạo truy vấn an toàn.",
                intent="clarification",
                confidence="medium",
            )
            _add(
                suggestions,
                label="Top brand",
                question="Top 5 brand theo doanh thu là gì?",
                reason="Brand là chiều phân tích phổ biến trong Gold summary.",
                intent="ranking",
                confidence="medium",
            )
            _add(
                suggestions,
                label="Theo thời gian",
                question="Doanh thu theo ngày là bao nhiêu?",
                reason="Chuỗi thời gian giúp làm rõ xu hướng khi câu hỏi còn chung chung.",
                intent="trend",
                confidence="medium",
            )
        return suggestions

    if not state.get("time_range") and intent in {"revenue_sales", "metric_overview", "ranking", "breakdown", "conversion_funnel"}:
        _add(
            suggestions,
            label="Ngày gần nhất",
            question=f"{metric_label.capitalize()} trong ngày gần nhất là bao nhiêu?",
            reason="Bổ sung thời gian để tránh đọc toàn bộ dữ liệu.",
            intent=intent or "metric_overview",
            confidence="high",
        )
        _add(
            suggestions,
            label="Theo ngày",
            question=f"{metric_label.capitalize()} theo ngày là bao nhiêu?",
            reason="Biến câu hỏi thành xu hướng thời gian dễ kiểm chứng.",
            intent="trend",
            confidence="high",
        )
        _add(
            suggestions,
            label="Tháng 1/2020",
            question=f"{metric_label.capitalize()} từ ngày 2020-01-01 đến ngày 2020-01-31 là bao nhiêu?",
            reason="Dùng khoảng ngày cụ thể có dạng Agent hiểu chắc chắn.",
            intent=intent or "metric_overview",
            confidence="medium",
        )

    if intent in {"ranking", "revenue_sales", "metric_overview"} and not dimension:
        _add(
            suggestions,
            label="Theo brand",
            question=f"Top 5 brand theo {metric_label} là gì?",
            reason="Brand có summary table riêng và thường trả lời tốt cho ranking.",
            intent="ranking",
            confidence="high",
        )
        _add(
            suggestions,
            label="Theo category",
            question=f"Top 5 category theo {metric_label} là gì?",
            reason="Category giúp thấy nhóm sản phẩm nổi bật.",
            intent="ranking",
            confidence="high",
        )
        _add(
            suggestions,
            label="Theo sản phẩm",
            question=f"Top 10 sản phẩm theo {metric_label} là gì?",
            reason="Product summary phù hợp khi cần drill-down xuống sản phẩm.",
            intent="ranking",
            confidence="medium",
        )

    if intent == "conversion_funnel" or _contains_any(text, ("conversion", "chuyển đổi", "chuyen doi", "funnel", "phễu", "pheu")):
        _add(
            suggestions,
            label="Phễu gần nhất",
            question="Phễu view đến cart đến purchase trong ngày gần nhất như thế nào?",
            reason="Bổ sung thời gian cho funnel để kết quả ổn định hơn.",
            intent="conversion_funnel",
            confidence="high",
        )
        _add(
            suggestions,
            label="Conversion brand",
            question="Top 5 brand theo conversion_rate là gì?",
            reason="So sánh conversion theo brand thay vì chỉ xem tổng quan.",
            intent="ranking",
            confidence="medium",
        )

    if int(state.get("row_count") or 0) == 0 and state.get("status") == "success":
        _add(
            suggestions,
            label="Nới thời gian",
            question="Tính cùng chỉ số đó trên toàn bộ dữ liệu Gold hiện có",
            reason="Kết quả rỗng thường do bộ lọc thời gian hoặc điều kiện quá hẹp.",
            intent=state.get("intent") or "metric_overview",
            confidence="high",
        )
        _add(
            suggestions,
            label="Dùng summary",
            question="Tổng quan event, view, cart, purchase theo ngày là bao nhiêu?",
            reason="Summary table thường ổn định hơn fact/detail khi debug kết quả rỗng.",
            intent="trend",
            confidence="medium",
        )

    if previous_dimension and previous_dimension != dimension:
        _add(
            suggestions,
            label=f"Còn {previous_dimension}",
            question=f"So sánh tiếp theo {previous_dimension} với cùng metric vừa hỏi",
            reason="Dựa trên ngữ cảnh hội thoại gần nhất.",
            intent="comparison",
            confidence="medium",
        )

    return suggestions[:5]
