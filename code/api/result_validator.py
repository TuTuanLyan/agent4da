from typing import Any


def _is_number(value: Any) -> bool:
    return not isinstance(value, bool) and isinstance(value, (int, float))


def _is_zero_number(value: Any) -> bool:
    return _is_number(value) and value == 0


def _is_revenue_column(column_name: str) -> bool:
    normalized = column_name.lower()
    return any(token in normalized for token in ("revenue", "amount", "doanh_thu", "doanhthu"))


def _uses_fact_sales(
    generated_sql: str,
    table_candidates: list[str],
    used_tables: list[str],
) -> bool:
    sql = generated_sql.lower()
    return (
        "fact_sales" in sql
        or "fact_sales" in table_candidates
        or any(table_ref.endswith(".fact_sales") or table_ref == "fact_sales" for table_ref in used_tables)
    )


def _has_time_column(rows: list[dict[str, Any]]) -> bool:
    if not rows:
        return False
    return any(
        any(token in column_name.lower() for token in ("date", "day", "hour", "time", "ts"))
        for column_name in rows[0]
    )


def _has_numeric_metric(row: dict[str, Any]) -> bool:
    for column_name, value in row.items():
        normalized = column_name.lower()
        if normalized.endswith("_id") or normalized == "id":
            continue
        if _is_number(value):
            return True
    return False


def validate_result(
    *,
    question: str,
    intent: str,
    generated_sql: str,
    rows: list[dict[str, Any]],
    row_count: int,
    table_candidates: list[str],
    used_tables: list[str],
) -> dict[str, Any]:
    warnings = []
    validation_notes = []
    confidence = "high"
    is_empty = not rows or row_count == 0

    if is_empty:
        warnings.append("Không có dữ liệu phù hợp với câu hỏi trong tập dữ liệu hiện tại.")
        validation_notes.append("Query returned zero rows.")
        confidence = "low"

    if intent == "revenue_sales":
        revenue_values = [
            value
            for row in rows
            for column_name, value in row.items()
            if _is_revenue_column(column_name) and _is_number(value)
        ]
        if revenue_values and all(_is_zero_number(value) for value in revenue_values):
            warnings.append(
                "Doanh thu đang bằng 0 trong dữ liệu hiện tại. Có thể tập dữ liệu test không có purchase event."
            )
            validation_notes.append("Revenue fields are present and all values are zero.")
            confidence = "medium"

        if _uses_fact_sales(generated_sql, table_candidates, used_tables) and row_count == 0:
            warnings.append("fact_sales hiện không có bản ghi purchase trong dữ liệu hiện tại.")
            validation_notes.append("fact_sales query returned zero rows.")
            confidence = "low"

    if intent == "ranking" and not is_empty:
        if _has_numeric_metric(rows[0]):
            validation_notes.append("Ranking result contains at least one numeric metric.")
            confidence = "high"
        else:
            warnings.append("Kết quả ranking không có metric dạng số rõ ràng.")
            validation_notes.append("No numeric ranking metric found in the first row.")
            confidence = "medium"

    if intent == "trend" and not is_empty and not _has_time_column(rows):
        warnings.append("Kết quả không có cột thời gian rõ ràng để phân tích xu hướng.")
        validation_notes.append("No date/time-like column found in trend result.")
        confidence = "medium"

    if intent == "drilldown":
        if row_count > 100:
            warnings.append("Kết quả drilldown có nhiều hơn 100 dòng; nên chỉ xem như preview một phần dữ liệu.")
            validation_notes.append("Drilldown result has more than 100 rows.")
            confidence = "medium"
        if "limit" not in generated_sql.lower():
            warnings.append("Truy vấn drilldown không có LIMIT rõ ràng.")
            validation_notes.append("Drilldown SQL has no LIMIT.")
            confidence = "medium"

    return {
        "is_empty": is_empty,
        "warnings": warnings,
        "confidence": confidence,
        "validation_notes": validation_notes,
    }
