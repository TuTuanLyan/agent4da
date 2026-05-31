import unicodedata


MAX_CHART_ROWS = 50
PIE_WORDS = ["ty le", "ti le", "phan tram", "share", "proportion"]
DUPLICATE_X_WARNING = "X-axis contains duplicated values. The result may be grouped by additional dimensions."


def normalize_text(text):
    text = unicodedata.normalize("NFD", (text or "").lower())
    return "".join(
        char for char in text
        if unicodedata.category(char) != "Mn"
    )


def should_use_pie_chart(question, row_count):
    if row_count > 6:
        return False

    normalized_question = normalize_text(question)
    return any(word in normalized_question for word in PIE_WORDS)


def choose_numeric_column(numeric_columns, excluded_columns):
    for column in numeric_columns:
        if column not in excluded_columns:
            return column

    if numeric_columns:
        return numeric_columns[0]

    return None


def has_duplicate_values(rows, column_name):
    seen = set()

    for row in rows:
        value = row.get(column_name)
        if value in seen:
            return True
        seen.add(value)

    return False


def add_duplicate_x_warning(chart_spec, rows):
    if chart_spec.get("type") not in ["bar", "pie"]:
        return chart_spec

    x_column = chart_spec.get("x")
    if x_column and has_duplicate_values(rows, x_column):
        chart_spec["warning"] = DUPLICATE_X_WARNING

    return chart_spec


def none_chart(reason):
    return {
        "type": "none",
        "title": "Không có biểu đồ",
        "x": None,
        "y": None,
        "data": [],
        "reason": reason
    }


def table_chart(rows, reason):
    return {
        "type": "table",
        "title": "Bảng dữ liệu kết quả",
        "x": None,
        "y": None,
        "data": rows[:MAX_CHART_ROWS],
        "reason": reason
    }


def plan_chart_node(state):
    if state.get("error"):
        return {
            "chart_spec": none_chart("Không tạo biểu đồ vì truy vấn đang có lỗi.")
        }

    rows = state.get("query_result") or []
    if not rows:
        return {
            "chart_spec": none_chart("Không có dữ liệu để vẽ biểu đồ.")
        }

    profile = state.get("result_profile") or {}
    row_count = profile.get("row_count", len(rows))
    numeric_columns = profile.get("numeric_columns", [])
    categorical_columns = profile.get("categorical_columns", [])
    time_columns = profile.get("time_columns", [])
    chart_rows = rows[:MAX_CHART_ROWS]

    if time_columns and numeric_columns:
        x_column = time_columns[0]
        y_column = choose_numeric_column(numeric_columns, [x_column])

        if y_column:
            return {
                "chart_spec": {
                    "type": "line",
                    "title": f"Xu hướng {y_column} theo {x_column}",
                    "x": x_column,
                    "y": y_column,
                    "data": chart_rows,
                    "reason": "Có cột thời gian và cột số nên phù hợp với biểu đồ đường."
                }
            }

    if categorical_columns and numeric_columns:
        x_column = categorical_columns[0]
        y_column = choose_numeric_column(numeric_columns, [x_column])

        if y_column:
            chart_type = "bar"
            title = f"So sánh {y_column} theo {x_column}"
            reason = "Có cột phân loại và cột số nên phù hợp với biểu đồ cột."

            if should_use_pie_chart(state.get("user_question"), row_count):
                chart_type = "pie"
                title = f"Tỷ lệ {y_column} theo {x_column}"
                reason = "Câu hỏi hỏi về tỷ lệ và số dòng ít nên phù hợp với biểu đồ tròn."

            chart_spec = {
                "type": chart_type,
                "title": title,
                "x": x_column,
                "y": y_column,
                "data": chart_rows,
                "reason": reason
            }

            return {
                "chart_spec": add_duplicate_x_warning(chart_spec, chart_rows)
            }

    return {
        "chart_spec": table_chart(
            rows,
            "Không tìm thấy cặp cột phù hợp cho line/bar/pie nên hiển thị dạng bảng."
        )
    }
