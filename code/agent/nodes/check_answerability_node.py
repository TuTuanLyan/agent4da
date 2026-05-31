import unicodedata


PRODUCT_NAME_TERMS = [
    "ten san pham",
    "tên sản phẩm",
    "product name",
    "name of product",
]

CHART_ONLY_TERMS = [
    "ve bieu do",
    "vẽ biểu đồ",
    "bieu do cot",
    "biểu đồ cột",
    "bar chart",
    "line chart",
    "pie chart",
]


def normalize_text(text):
    text = unicodedata.normalize("NFD", (text or "").lower())
    normalized = "".join(
        char for char in text
        if unicodedata.category(char) != "Mn"
    )
    return normalized.replace("đ", "d")


def asks_product_name(question):
    normalized = normalize_text(question)
    if any(normalize_text(term) in normalized for term in PRODUCT_NAME_TERMS):
        return True
    return "san pham" in normalized and "ten" in normalized


def asks_chart_only(question):
    normalized = normalize_text(question)
    if not any(term in normalized for term in CHART_ONLY_TERMS):
        return False

    data_terms = [
        "doanh thu",
        "revenue",
        "luot xem",
        "lượt xem",
        "brand",
        "nhan hang",
        "nhãn hàng",
        "san pham",
        "sản phẩm",
    ]
    return not any(normalize_text(term) in normalized for term in data_terms)


def has_previous_context(state):
    app_context = state.get("app_context") or {}
    return bool(app_context.get("last_sql") or app_context.get("last_question"))


def check_answerability_node(state):
    question = state.get("user_question") or ""

    if asks_product_name(question):
        return {
            "answer_kind": "no_data",
            "text_answer": (
                "Dữ liệu Gold hiện chỉ có product_id, brand và category cho sản phẩm; "
                "không có cột tên sản phẩm/product_name nên mình không thể trả lời "
                "tên sản phẩm một cách chính xác."
            ),
            "query_result": [],
            "stop_reason": "missing_product_name_column",
            "stop_after_answerability": True,
            "error": None,
        }

    if asks_chart_only(question) and not has_previous_context(state):
        return {
            "answer_kind": "clarification",
            "text_answer": (
                "Bạn muốn vẽ biểu đồ cho chỉ số nào? Ví dụ: top brand theo lượt xem "
                "hoặc doanh thu theo ngày."
            ),
            "query_result": [],
            "stop_reason": "chart_request_without_context",
            "stop_after_answerability": True,
            "error": None,
        }

    return {
        "stop_after_answerability": False,
    }
