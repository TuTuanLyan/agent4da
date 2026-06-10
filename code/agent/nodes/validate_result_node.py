import re
import unicodedata


DIRECT_TEXT_FILTER_RE = re.compile(
    r"\b(brand|category_l1|category_l2|category_l3|event_type)\b\s*(=|<>|!=)\s*'([^']+)'",
    re.IGNORECASE,
)


def normalize_text(text):
    text = unicodedata.normalize("NFD", (text or "").lower())
    normalized = "".join(
        char for char in text
        if unicodedata.category(char) != "Mn"
    )
    return normalized.replace("đ", "d")


def column_matches(columns, candidates):
    normalized_columns = [normalize_text(column) for column in columns]
    for candidate in candidates:
        normalized_candidate = normalize_text(candidate)
        for column in normalized_columns:
            if normalized_candidate in column:
                return True
    return False


def direct_text_filters_needing_normalization(sql):
    filters = []
    for column_name, operator, value in DIRECT_TEXT_FILTER_RE.findall(sql or ""):
        stripped_value = value.strip()
        if stripped_value != stripped_value.lower() or stripped_value != value:
            filters.append(
                {
                    "column": column_name,
                    "operator": operator,
                    "value": value,
                }
            )
    return filters


def expected_fields(question):
    normalized = normalize_text(question)
    fields = []

    if any(term in normalized for term in ["brand", "nhan hang", "thuong hieu", "hang"]):
        fields.append(
            {
                "name": "brand",
                "column_candidates": ["brand"],
                "reason": "Câu hỏi yêu cầu thông tin nhãn hàng/brand.",
            }
        )

    if any(term in normalized for term in ["san pham", "product"]):
        fields.append(
            {
                "name": "product_id",
                "column_candidates": ["product_id"],
                "reason": "Câu hỏi yêu cầu thông tin sản phẩm; Gold hiện định danh bằng product_id.",
            }
        )

    if any(term in normalized for term in ["luot xem", "view", "views"]):
        fields.append(
            {
                "name": "view_metric",
                "column_candidates": ["view", "views", "view_count", "total_views"],
                "reason": "Câu hỏi yêu cầu metric lượt xem.",
            }
        )

    return fields


def missing_expected_fields(question, columns):
    missing = []
    for field in expected_fields(question):
        if not column_matches(columns, field["column_candidates"]):
            missing.append(field)
    return missing


def validate_result_node(state):
    if state.get("error"):
        return {}

    rows = state.get("query_result") or []
    profile = state.get("result_profile") or {}
    columns = profile.get("columns") or (list(rows[0].keys()) if rows else [])

    if not rows:
        suspect_filters = direct_text_filters_needing_normalization(state.get("generated_sql") or "")
        requery_count = int(state.get("requery_count") or 0)
        max_requery_rounds = int(state.get("max_requery_rounds") or 1)
        can_requery = bool(suspect_filters) and requery_count < max_requery_rounds

        if can_requery:
            return {
                "result_validation": {
                    "valid": False,
                    "can_requery": True,
                    "missing_fields": [],
                    "suspect_filters": suspect_filters,
                    "notes": (
                        "Query returned no rows and used direct case-sensitive text filters. "
                        "Rewrite user-provided text filters with lower(trim(column)) comparisons."
                    ),
                },
                "requery_requested": True,
                "requery_count": requery_count + 1,
            }

        return {
            "result_validation": {
                "valid": True,
                "can_requery": False,
                "missing_fields": [],
                "notes": "Query returned no rows; insight node will produce no_data response.",
            },
            "requery_requested": False,
        }

    missing = missing_expected_fields(state.get("user_question") or "", columns)
    if not missing:
        return {
            "result_validation": {
                "valid": True,
                "can_requery": False,
                "missing_fields": [],
                "notes": "",
            },
            "requery_requested": False,
        }

    requery_count = int(state.get("requery_count") or 0)
    max_requery_rounds = int(state.get("max_requery_rounds") or 1)
    can_requery = requery_count < max_requery_rounds
    notes = (
        "Query ran successfully but result is missing fields needed by the question: "
        + ", ".join(item["name"] for item in missing)
    )

    return {
        "result_validation": {
            "valid": not can_requery,
            "can_requery": can_requery,
            "missing_fields": missing,
            "notes": notes,
            "previous_columns": columns,
        },
        "requery_requested": can_requery,
        "requery_count": requery_count + 1 if can_requery else requery_count,
    }
