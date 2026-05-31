"""Rule-based NLU / intent parser (ported from code/api/nlu_parser.py).

Pure Python, no external dependencies, no catalog references. Extracts intent,
dimension, metric, time range/grain, sort direction, comparison entities, and
table candidates from a Vietnamese/English business question.
"""

from __future__ import annotations

import re
from typing import Any

from .config import GOLD_TABLES as KNOWN_TABLES

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
    # "nhãn hàng" / "nhãn hiệu" are common Vietnamese words for brand; keep the
    # accented "nhãn" but not bare ASCII "nhan" (collides with "nhanh" = fast).
    if _contains_any(
        text,
        (
            "brand",
            "hãng",
            "hang",
            "thương hiệu",
            "thuong hieu",
            "nhãn hàng",
            "nhan hang",
            "nhãn hiệu",
            "nhan hieu",
            "nhãn",
        ),
    ):
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
    return {"type": "date_range", "field": "event_date", "start": start, "end": end}


def _exact_date(value: str) -> dict[str, str]:
    return {"type": "exact_date", "field": "event_date", "start": value, "end": value}


def _symbolic_time_range(range_type: str) -> dict[str, str]:
    return {"type": range_type, "field": "event_date"}


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

    # A bare/era year ("trong 2020", "năm 2020", "in 2019") -> full-year range.
    # Runs last so explicit yyyy-mm-dd dates above win.
    year_match = re.search(r"\b(19|20)\d{2}\b", text)
    if year_match:
        year = year_match.group(0)
        return _date_range(f"{year}-01-01", f"{year}-12-31")
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


# Exclusion follow-ups ("bỏ qua nhãn hàng unknown", "loại trừ apple, sony") map
# to NOT IN filters. The dimension words are stripped so only the entity values
# remain.
_EXCLUSION_RE = re.compile(
    r"(?:bỏ qua|bo qua|loại trừ|loai tru|loại bỏ|loai bo|không tính|khong tinh|"
    r"không bao gồm|khong bao gom|ngoại trừ|ngoai tru|exclude|except|without)\s+(.+)$"
)
# Dimension words to strip from an exclusion phrase, longest-first so multi-word
# phrases are removed before their fragments. Accented + ASCII so it works on
# either spelling.
_DIMENSION_PHRASES = (
    "nhãn hàng", "nhan hang", "nhãn hiệu", "nhan hieu",
    "thương hiệu", "thuong hieu", "danh mục", "danh muc",
    "sản phẩm", "san pham",
    "nhãn", "nhan", "hãng", "hang", "brand", "category", "product",
)


def _filter_field_from_text(text: str, fallback: str | None) -> str:
    if _contains_any(
        text,
        ("nhãn hàng", "nhan hang", "nhãn hiệu", "nhan hieu", "nhãn", "thương hiệu", "thuong hieu", "hãng", "hang", "brand"),
    ):
        return "brand"
    if _contains_any(text, ("category", "danh mục", "danh muc")):
        return "category"
    if _contains_any(text, ("sản phẩm", "san pham", "product")):
        return "product"
    return fallback or "brand"


def extract_exclusion_filters(question: str, dimension: str | None = None) -> list[dict[str, Any]]:
    """Parse "bỏ qua/loại trừ/không tính/ngoại trừ <X>" into a NOT IN filter.

    The matched dimension words are dropped so only the entity values remain,
    e.g. "bỏ qua nhãn hàng unknown" -> [{field: brand, operator: not_in,
    values: ["unknown"]}]. Returns [] when no exclusion phrase is present.
    """
    text = question.strip().lower()
    match = _EXCLUSION_RE.search(text)
    if not match:
        return []
    tail = match.group(1).strip()
    field = _filter_field_from_text(tail, dimension)
    # Strip dimension words (keeps entity values, incl. non-ASCII brand names).
    cleaned = tail
    for phrase in _DIMENSION_PHRASES:
        cleaned = cleaned.replace(phrase, " ")
    chunks = re.split(r"\s*(?:,|;|/|\bvà\b|\bva\b|\band\b)\s*", cleaned)
    values: list[str] = []
    for chunk in chunks:
        value = re.sub(r"\s+", " ", chunk).strip(" \t.,;:!?-")
        if value:
            values.append(value)
    if not values:
        return []
    return [{"field": field, "operator": "not_in", "values": values}]


_TIME_WORDS = ("năm", "nam", "tháng", "thang", "ngày", "ngay", "quý", "quy", "tuần", "tuan", "year", "month", "quarter", "week", "hôm", "hom")

# Note: bare ASCII "chi" is intentionally excluded - it collides with "chi tiết"
# (detail), "chi phí" (cost), etc. Accented "chỉ" is unambiguous.
_INCLUSION_RE = re.compile(
    r"(?:^|\b)(?:chỉ|riêng|rieng|duy nhất|duy nhat|only|just)\s+(.+)$"
)
_THRESHOLD_WORDS = ("trên", "tren", "dưới", "duoi", "lớn hơn", "lon hon", "nhỏ hơn", "nho hon", "ít nhất", "it nhat", "tối đa", "toi da", "tối thiểu", "toi thieu", ">", "<", "≥", "≤")


def _looks_like_time_or_limit(tail: str) -> bool:
    if re.search(r"\b(19|20)\d{2}\b", tail):
        return True
    if _contains_any(tail, _TIME_WORDS):
        return True
    if re.match(r"^\s*(?:top\s+)?\d", tail):
        return True
    return False


def extract_inclusion_filters(question: str, dimension: str | None = None) -> list[dict[str, Any]]:
    """Parse "chỉ / riêng / only <X>" into an IN filter (restrict to X).

    Skips time ("chỉ năm 2021") and limit ("chỉ 5", "chỉ top 5") tails, which are
    not entity restrictions. Returns [] when no inclusion entity is present.
    """
    text = question.strip().lower()
    match = _INCLUSION_RE.search(text)
    if not match:
        return []
    tail = match.group(1).strip()
    # "chỉ ... trên 1000" is a numeric threshold, not an entity restriction.
    if _looks_like_time_or_limit(tail) or _contains_any(tail, _THRESHOLD_WORDS) or extract_threshold_filters(text):
        return []
    field = _filter_field_from_text(tail, dimension)
    cleaned = tail
    for phrase in _DIMENSION_PHRASES:
        cleaned = cleaned.replace(phrase, " ")
    chunks = re.split(r"\s*(?:,|;|/|\bvà\b|\bva\b|\band\b)\s*", cleaned)
    values: list[str] = []
    for chunk in chunks:
        value = re.sub(r"\s+", " ", chunk).strip(" \t.,;:!?-")
        # Drop trailing analytic words that are not entity names.
        if value and value not in ("thôi", "thoi", "nhé", "nhe", "thì sao", "thi sao"):
            values.append(value)
    if not values:
        return []
    return [{"field": field, "operator": "in", "values": values}]


_NUMBER_UNIT_RE = re.compile(
    r"(\d[\d.,]*)\s*(tỷ|ty|triệu|trieu|nghìn|nghin|tr|k|m|b)?",
    flags=re.IGNORECASE,
)
_UNIT_MULTIPLIER = {
    "k": 1_000, "nghìn": 1_000, "nghin": 1_000,
    "tr": 1_000_000, "triệu": 1_000_000, "trieu": 1_000_000, "m": 1_000_000,
    "tỷ": 1_000_000_000, "ty": 1_000_000_000, "b": 1_000_000_000,
}

_THRESHOLD_PATTERNS = (
    (">=", r"(?:ít nhất|it nhat|tối thiểu|toi thieu|từ|tu|>=|≥|at least)\s+"),
    ("<=", r"(?:tối đa|toi da|nhiều nhất là|nhieu nhat la|<=|≤|at most)\s+"),
    (">", r"(?:trên|tren|lớn hơn|lon hon|hơn|hon|>|greater than|more than|over)\s+"),
    ("<", r"(?:dưới|duoi|nhỏ hơn|nho hon|ít hơn|it hon|<|less than|under|below)\s+"),
)


def _parse_number(token: str, unit: str | None) -> float | None:
    cleaned = token.replace(",", "").rstrip(".")
    try:
        value = float(cleaned)
    except ValueError:
        return None
    if unit:
        value *= _UNIT_MULTIPLIER.get(unit.lower(), 1)
    return value


def extract_threshold_filters(question: str) -> list[dict[str, Any]]:
    """Parse numeric thresholds ("trên 1000", "ít nhất 100", "dưới 1 triệu").

    These apply to the question's metric, so the field is the sentinel
    ``"__metric__"`` resolved to the aggregate column at SQL-build time (HAVING).
    """
    text = question.strip().lower()
    out: list[dict[str, Any]] = []
    for operator, prefix in _THRESHOLD_PATTERNS:
        for match in re.finditer(prefix + _NUMBER_UNIT_RE.pattern, text):
            number = match.group(1)
            unit = match.group(2)
            # A bare 4-digit year with no unit is a time filter, not a threshold.
            if not unit and re.fullmatch(r"(?:19|20)\d{2}", number.replace(",", "").rstrip(".")):
                continue
            value = _parse_number(number, unit)
            if value is None:
                continue
            out.append({"field": "__metric__", "operator": operator, "values": [value]})
    return out


def analyze_message(question: str, dimension_hint: str | None = None) -> dict[str, Any]:
    """All NLU signals present in a (possibly elliptical) message.

    Used by the follow-up classifier to build a typed patch. Unlike ``parse_nlu``
    it does not force an intent; it just reports what the message mentions.
    """
    text = question.strip().lower()
    dimension = _extract_dimension(text) or dimension_hint
    return {
        "text": text,
        "metric": _extract_metric(text),
        "metrics": _extract_metrics(text),
        "dimension": _extract_dimension(text),
        "time_range": _extract_time_range(text),
        "time_grain": _extract_time_grain(text),
        "sort_direction": _extract_sort_direction(text),
        "limit": _extract_limit_explicit(text),
        "exclusion_filters": extract_exclusion_filters(text, dimension),
        "inclusion_filters": extract_inclusion_filters(text, dimension),
        "threshold_filters": extract_threshold_filters(text),
        "comparison_entities": _extract_comparison_entities(text),
    }


def _extract_limit_explicit(text: str) -> int | None:
    """Like _extract_limit but returns None when no limit is stated."""
    match = re.search(r"\b(?:top|limit|lấy|lay|xem|liệt kê|liet ke|chỉ|chi)\s+(\d{1,3})\b", text)
    if not match:
        match = re.search(r"\b(\d{1,3})\s+(?:thôi|thoi|cái|cai)\b", text)
    if not match:
        return None
    return max(1, min(int(match.group(1)), 100))


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
    exclusion_filters = extract_exclusion_filters(text, dimension)
    inclusion_filters = extract_inclusion_filters(text, dimension)
    threshold_filters = extract_threshold_filters(text)
    extra_filters = exclusion_filters + inclusion_filters + threshold_filters

    extracted_entities = {
        "metrics": metrics,
        "dimension": dimension,
        "time_grain": time_grain,
        "time_range": time_range,
        "comparison_entities": comparison_entities,
        "filters": extra_filters,
    }

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
            filters=[{"field": dimension, "operator": "in", "values": comparison_entities}] + extra_filters,
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
            filters=extra_filters,
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
            filters=extra_filters,
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
            filters=extra_filters,
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
            filters=extra_filters,
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
            filters=extra_filters,
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
            filters=extra_filters,
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
            filters=extra_filters,
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


def route_intent(question: str) -> dict[str, Any]:
    return parse_nlu(question)
