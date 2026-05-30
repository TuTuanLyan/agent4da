import re
from typing import Any


KNOWN_TABLES = [
    "daily_brand_summary",
    "daily_category_summary",
    "daily_event_summary",
    "daily_product_summary",
    "dim_product",
    "dim_session",
    "dim_time",
    "dim_user",
    "fact_events",
    "fact_sales",
]

METRIC_KEYWORDS = (
    ("revenue", ("doanh thu", "revenue", "sales", "gross_amount")),
    ("total_views", ("view", "lượt xem", "luot xem")),
    ("total_carts", ("cart", "giỏ hàng", "gio hang")),
    ("total_purchases", ("purchase", "mua hàng", "mua hang")),
    ("total_events", ("event", "sự kiện", "su kien", "lượt tương tác", "luot tuong tac")),
    ("count", ("số lượng", "so luong", "count", "cnt")),
)

BRAND_ENTITIES = ("apple", "samsung", "xiaomi", "huawei", "oppo", "sony", "lg", "lenovo")


def _contains_any(text: str, keywords: tuple[str, ...]) -> bool:
    return any(keyword in text for keyword in keywords)


def _extract_limit(text: str, default: int = 10) -> int:
    match = re.search(r"\b(?:top|limit|lấy|lay|xem|liệt kê|liet ke)\s+(\d{1,3})\b", text)
    if not match:
        match = re.search(r"\b(\d{1,3})\s+thôi\b", text)
    if not match:
        return default
    return max(1, min(int(match.group(1)), 100))


def _extract_table_name(text: str) -> str | None:
    for table in KNOWN_TABLES:
        if table in text:
            return table
    return None


def _extract_metric(text: str) -> str | None:
    for metric, keywords in METRIC_KEYWORDS:
        if _contains_any(text, keywords):
            return metric
    return None


def _extract_metrics(text: str) -> list[str]:
    metrics = []
    for metric, keywords in METRIC_KEYWORDS:
        if _contains_any(text, keywords):
            metrics.append(metric)
    return metrics


def _extract_dimension(text: str) -> str | None:
    if _contains_any(text, ("brand", "hãng", "hang", "thương hiệu", "thuong hieu")):
        return "brand"
    if _contains_any(text, ("category", "danh mục", "danh muc")):
        return "category"
    if _contains_any(text, ("product", "sản phẩm", "san pham")):
        return "product"
    if _contains_any(text, ("event type", "event_type", "loại sự kiện", "loai su kien")):
        return "event_type"
    if _contains_any(text, ("ngày", "ngay", "thời gian", "thoi gian", "date")):
        return "event_date"
    return None


def _extract_time_grain(text: str) -> str | None:
    if _contains_any(text, ("theo giờ", "theo gio", "mỗi giờ", "moi gio", "hourly")):
        return "hour"
    if _contains_any(text, ("theo tháng", "theo thang", "mỗi tháng", "moi thang", "monthly")):
        return "month"
    if _contains_any(text, ("theo ngày", "theo ngay", "mỗi ngày", "moi ngay", "daily")):
        return "day"
    return None


def _date_range(start: str, end: str) -> dict[str, str]:
    return {
        "type": "date_range",
        "field": "event_date",
        "start": start,
        "end": end,
    }


def _exact_date(value: str) -> dict[str, str]:
    return {
        "type": "exact_date",
        "field": "event_date",
        "start": value,
        "end": value,
    }


def _symbolic_time_range(range_type: str) -> dict[str, str]:
    return {
        "type": range_type,
        "field": "event_date",
    }


def _extract_time_range(text: str) -> dict[str, str] | None:
    date_range_match = re.search(
        r"(?:từ ngày|tu ngay|from)\s+(\d{4}-\d{2}-\d{2})\s+"
        r"(?:đến ngày|den ngay|to)\s+(\d{4}-\d{2}-\d{2})",
        text,
    )
    if date_range_match:
        return _date_range(date_range_match.group(1), date_range_match.group(2))

    exact_date_match = re.search(r"(?:trong ngày|trong ngay|ngày|ngay|date)\s+(\d{4}-\d{2}-\d{2})", text)
    if exact_date_match:
        return _exact_date(exact_date_match.group(1))

    if _contains_any(text, ("ngày gần nhất", "ngay gan nhat", "latest day", "gần nhất", "gan nhat")):
        return _symbolic_time_range("latest")
    if _contains_any(text, ("hôm nay", "hom nay", "today")):
        return _symbolic_time_range("today")
    if _contains_any(text, ("hôm qua", "hom qua", "yesterday")):
        return _symbolic_time_range("yesterday")
    if _contains_any(text, ("tuần này", "tuan nay", "this week")):
        return _symbolic_time_range("this_week")
    return None


def _extract_sort_direction(text: str) -> str | None:
    if _contains_any(text, ("thấp nhất", "thap nhat", "ít nhất", "it nhat", "lowest", "smallest")):
        return "asc"
    if _contains_any(text, ("top", "cao nhất", "cao nhat", "nhiều nhất", "nhieu nhat", "lớn nhất", "lon nhat", "highest", "best")):
        return "desc"
    if _contains_any(text, ("nhiều", "nhieu", "lớn", "lon")) and _contains_any(text, ("nhất", "nhat")):
        return "desc"
    if _contains_any(text, ("ít", "it", "thấp", "thap")) and _contains_any(text, ("nhất", "nhat")):
        return "asc"
    return None


def _clean_entity(entity: str) -> str:
    entity = entity.strip().lower()
    entity = re.sub(r"^(brand|hãng|hang|thương hiệu|thuong hieu)\s+", "", entity)
    entity = re.sub(r"\s+(theo|về|ve|bằng|bang|với|voi).*$", "", entity)
    entity = re.sub(r"[^a-z0-9_\- ]+", "", entity)
    return entity.strip()


def _extract_comparison_entities(text: str) -> list[str]:
    for left, right in re.findall(r"\b([a-z0-9_\-]+)\s+vs\.?\s+([a-z0-9_\-]+)\b", text):
        return [_clean_entity(left), _clean_entity(right)]

    patterns = (
        r"so sánh\s+(.+?)\s+(?:và|va|voi|với)\s+(.+?)(?:\s+theo|\s+về|\s+ve|$)",
        r"giữa\s+(.+?)\s+(?:và|va)\s+(.+?)(?:\s+theo|\s+về|\s+ve|$)",
    )
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            entities = [_clean_entity(match.group(1)), _clean_entity(match.group(2))]
            return [entity for entity in entities if entity]

    mentioned_brands = [brand for brand in BRAND_ENTITIES if re.search(rf"\b{brand}\b", text)]
    return mentioned_brands[:2] if len(mentioned_brands) >= 2 else []


def _table_candidates(dimension: str | None, metric: str | None, intent: str, time_grain: str | None) -> list[str]:
    if intent == "comparison":
        return ["daily_brand_summary"] if dimension == "brand" else ["daily_event_summary"]
    if intent == "breakdown":
        return ["daily_event_summary"]
    if intent == "conversion_funnel":
        return ["daily_event_summary"]
    if intent == "drilldown":
        return ["fact_sales"] if metric == "revenue" else ["fact_events"]
    if metric == "revenue" and time_grain:
        return ["daily_event_summary"]
    if dimension == "brand":
        return ["daily_brand_summary"]
    if dimension == "category":
        return ["daily_category_summary"]
    if dimension == "product":
        return ["daily_product_summary"]
    return ["daily_event_summary"]


def _base_result(
    *,
    intent: str,
    dimension: str | None,
    metric: str | None,
    table_candidates: list[str],
    limit: int,
    needs_metadata: bool,
    table_name: str | None,
    analysis_type: str,
    time_range: dict[str, Any] | None,
    time_grain: str | None,
    filters: list[dict[str, Any]] | None = None,
    comparison_entities: list[str] | None = None,
    sort_direction: str | None = None,
    extracted_entities: dict[str, Any] | None = None,
    nlu_confidence: str = "high",
) -> dict[str, Any]:
    return {
        "intent": intent,
        "dimension": dimension,
        "metric": metric,
        "table_candidates": table_candidates,
        "limit": limit,
        "needs_metadata": needs_metadata,
        "table_name": table_name,
        "analysis_type": analysis_type,
        "time_range": time_range,
        "time_grain": time_grain,
        "filters": filters or [],
        "comparison_entities": comparison_entities or [],
        "sort_direction": sort_direction,
        "extracted_entities": extracted_entities or {},
        "nlu_confidence": nlu_confidence,
    }


def parse_nlu(question: str) -> dict[str, Any]:
    text = question.strip().lower()
    limit = _extract_limit(text)
    table_name = _extract_table_name(text)
    metric = _extract_metric(text)
    metrics = _extract_metrics(text)
    dimension = _extract_dimension(text)
    time_grain = _extract_time_grain(text)
    time_range = _extract_time_range(text)
    sort_direction = _extract_sort_direction(text)
    comparison_entities = _extract_comparison_entities(text)

    extracted_entities = {
        "metrics": metrics,
        "dimension": dimension,
        "time_grain": time_grain,
        "time_range": time_range,
        "comparison_entities": comparison_entities,
    }

    asks_business_metadata = (
        _contains_any(text, ("metadata", "semantic", "business metadata", "metadata business", "ngữ nghĩa", "ngu nghia", "nghiệp vụ", "nghiep vu"))
        and _contains_any(text, ("bảng", "bang", "table", "tables", "nào", "nao", "có", "co", "hệ thống", "he thong"))
    )
    if asks_business_metadata:
        return _base_result(
            intent="metadata_business",
            dimension=None,
            metric=None,
            table_candidates=[],
            limit=limit,
            needs_metadata=True,
            table_name=None,
            analysis_type="metadata",
            time_range=time_range,
            time_grain=time_grain,
            sort_direction=sort_direction,
            extracted_entities=extracted_entities,
        )

    asks_gold_tables = (
        "bảng gold" in text
        or "bang gold" in text
        or ("gold" in text and _contains_any(text, ("bảng", "bang", "table", "tables")))
    )
    if asks_gold_tables and _contains_any(text, ("nào", "nao", "liệt kê", "liet ke", "danh sách", "danh sach", "có", "co")):
        return _base_result(
            intent="metadata_tables",
            dimension=None,
            metric=None,
            table_candidates=[],
            limit=limit,
            needs_metadata=True,
            table_name=None,
            analysis_type="metadata",
            time_range=time_range,
            time_grain=time_grain,
            sort_direction=sort_direction,
            extracted_entities=extracted_entities,
        )

    asks_columns = _contains_any(text, ("cột", "cot", "column", "columns", "field", "fields", "schema"))
    if asks_columns and table_name:
        return _base_result(
            intent="metadata_columns",
            dimension=None,
            metric=None,
            table_candidates=[table_name],
            limit=limit,
            needs_metadata=True,
            table_name=table_name,
            analysis_type="metadata",
            time_range=time_range,
            time_grain=time_grain,
            sort_direction=sort_direction,
            extracted_entities=extracted_entities,
        )

    if _contains_any(text, ("thời tiết", "thoi tiet", "hà nội", "ha noi", "hanoi", "weather")):
        return _base_result(
            intent="unsupported",
            dimension=None,
            metric=None,
            table_candidates=[],
            limit=limit,
            needs_metadata=False,
            table_name=table_name,
            analysis_type="unsupported",
            time_range=time_range,
            time_grain=time_grain,
            sort_direction=sort_direction,
            extracted_entities=extracted_entities,
            nlu_confidence="high",
        )

    if comparison_entities:
        dimension = dimension or "brand"
        metric = metric or "total_events"
        return _base_result(
            intent="comparison",
            dimension=dimension,
            metric=metric,
            table_candidates=_table_candidates(dimension, metric, "comparison", time_grain),
            limit=limit,
            needs_metadata=True,
            table_name=table_name,
            analysis_type="comparison",
            time_range=time_range,
            time_grain=time_grain,
            filters=[{"field": dimension, "operator": "in", "values": comparison_entities}],
            comparison_entities=comparison_entities,
            sort_direction=sort_direction or "desc",
            extracted_entities=extracted_entities,
        )

    if _contains_any(text, ("funnel", "phễu", "pheu", "view đến cart", "view den cart", "cart đến purchase", "cart den purchase")):
        return _base_result(
            intent="conversion_funnel",
            dimension=None,
            metric="conversion",
            table_candidates=["daily_event_summary"],
            limit=limit,
            needs_metadata=True,
            table_name=table_name,
            analysis_type="funnel",
            time_range=time_range,
            time_grain=time_grain,
            sort_direction=sort_direction,
            extracted_entities=extracted_entities,
        )

    if _contains_any(text, ("tỷ trọng", "ty trong", "cơ cấu", "co cau", "phân bổ", "phan bo", "breakdown")):
        return _base_result(
            intent="breakdown",
            dimension=dimension,
            metric=metric or (metrics[0] if metrics else "total_events"),
            table_candidates=_table_candidates(dimension, metric, "breakdown", time_grain),
            limit=limit,
            needs_metadata=True,
            table_name=table_name,
            analysis_type="breakdown",
            time_range=time_range,
            time_grain=time_grain,
            sort_direction=sort_direction or "desc",
            extracted_entities=extracted_entities,
        )

    is_drilldown = _contains_any(text, ("xem", "liệt kê", "liet ke", "sample", "chi tiết", "chi tiet"))
    if is_drilldown and _contains_any(text, ("event", "fact_events", "sample", "chi tiết", "chi tiet", "gần nhất", "gan nhat", "sale", "sales", "purchase", "doanh thu", "revenue")):
        metric = metric or ("revenue" if _contains_any(text, ("sale", "sales", "doanh thu", "revenue")) else "total_events")
        return _base_result(
            intent="drilldown",
            dimension=dimension,
            metric=metric,
            table_candidates=_table_candidates(dimension, metric, "drilldown", time_grain),
            limit=limit,
            needs_metadata=True,
            table_name=table_name,
            analysis_type="drilldown",
            time_range=time_range,
            time_grain=time_grain,
            sort_direction=sort_direction or "desc",
            extracted_entities=extracted_entities,
        )

    is_revenue = metric == "revenue"
    is_ranking = sort_direction in {"asc", "desc"} and (
        _contains_any(text, ("top", "cao nhất", "cao nhat", "thấp nhất", "thap nhat", "ít nhất", "it nhat"))
        or (_contains_any(text, ("nhiều", "nhieu")) and _contains_any(text, ("nhất", "nhat")))
    )

    if time_grain and is_revenue:
        return _base_result(
            intent="revenue_sales",
            dimension="event_date",
            metric="revenue",
            table_candidates=["daily_event_summary"],
            limit=limit,
            needs_metadata=True,
            table_name=table_name,
            analysis_type="time_series",
            time_range=time_range,
            time_grain=time_grain,
            sort_direction=sort_direction,
            extracted_entities=extracted_entities,
        )

    if time_grain:
        return _base_result(
            intent="trend",
            dimension="event_date",
            metric=metric or "total_events",
            table_candidates=["daily_event_summary"],
            limit=limit,
            needs_metadata=True,
            table_name=table_name,
            analysis_type="time_series",
            time_range=time_range,
            time_grain=time_grain,
            sort_direction=sort_direction,
            extracted_entities=extracted_entities,
        )

    if is_revenue:
        return _base_result(
            intent="revenue_sales",
            dimension=dimension,
            metric="revenue",
            table_candidates=_table_candidates(dimension, "revenue", "revenue_sales", time_grain),
            limit=limit,
            needs_metadata=True,
            table_name=table_name,
            analysis_type="revenue_summary" if not dimension else "topk",
            time_range=time_range,
            time_grain=time_grain,
            sort_direction=sort_direction or ("desc" if dimension else None),
            extracted_entities=extracted_entities,
        )

    if is_ranking:
        metric = metric or "total_events"
        return _base_result(
            intent="ranking",
            dimension=dimension,
            metric=metric,
            table_candidates=_table_candidates(dimension, metric, "ranking", time_grain),
            limit=limit,
            needs_metadata=True,
            table_name=table_name,
            analysis_type="topk",
            time_range=time_range,
            time_grain=time_grain,
            sort_direction=sort_direction or "desc",
            extracted_entities=extracted_entities,
        )

    if _contains_any(text, ("bao nhiêu", "bao nhieu", "tổng", "tong", "count", "số", "so")) and (
        metric or _contains_any(text, ("user", "session"))
    ):
        return _base_result(
            intent="metric_overview",
            dimension=None,
            metric=metric or "total_events",
            table_candidates=["daily_event_summary"],
            limit=limit,
            needs_metadata=True,
            table_name=table_name,
            analysis_type="overview",
            time_range=time_range,
            time_grain=time_grain,
            sort_direction=sort_direction,
            extracted_entities=extracted_entities,
        )

    return _base_result(
        intent="unsupported",
        dimension=None,
        metric=None,
        table_candidates=[],
        limit=limit,
        needs_metadata=False,
        table_name=table_name,
        analysis_type="unsupported",
        time_range=time_range,
        time_grain=time_grain,
        sort_direction=sort_direction,
        extracted_entities=extracted_entities,
        nlu_confidence="medium",
    )
