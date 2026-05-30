"""Chart recommender (ported verbatim from code/api/chart_recommender.py).

Pure Python. Given the intent and result rows, returns a chart recommendation
dict with normalized `data` (capped to a small number of rows for the UI).
"""

from __future__ import annotations

from decimal import Decimal
from numbers import Number
from typing import Any

DIMENSION_COLUMNS = (
    "brand",
    "category_code",
    "category_l1",
    "category_l2",
    "category_l3",
    "product_id",
    "product",
    "event_type",
)
TIME_COLUMNS = ("event_date", "date", "day", "hour", "event_hour", "sale_date")
NUMERIC_METRIC_COLUMNS = (
    "unique_events",
    "total_events",
    "total_views",
    "total_carts",
    "total_purchases",
    "view_count",
    "cart_count",
    "purchase_count",
    "revenue",
    "total_revenue",
    "gross_amount",
    "count",
    "cnt",
    "event_count",
)
OVERVIEW_EVENT_METRICS = (
    "total_views",
    "total_carts",
    "total_purchases",
    "view_count",
    "cart_count",
    "purchase_count",
)
DEFAULT_CHART_LIMIT = 20


def _is_number(value: Any) -> bool:
    return not isinstance(value, bool) and isinstance(value, Number)


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if _is_number(value):
        return value
    if value is None or isinstance(value, str):
        return value
    return str(value)


def _number_value(value: Any) -> int | float | None:
    if not _is_number(value):
        return None
    if isinstance(value, Decimal):
        return float(value)
    return value


def _empty_chart(reason: str) -> dict[str, Any]:
    return {
        "recommended": False,
        "type": None,
        "title": None,
        "x": None,
        "y": None,
        "series": None,
        "reason": reason,
        "limit": None,
        "chart_data_mode": None,
        "data": [],
        "columns": {},
        "alternative_types": [],
    }


def _chart(
    *,
    chart_type: str,
    title: str,
    x: str,
    y: str,
    reason: str,
    limit: int | None,
    chart_data_mode: str | None = None,
    series: str | None = None,
    data: list[dict[str, Any]] | None = None,
    columns: dict[str, str | None] | None = None,
    alternative_types: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "recommended": True,
        "type": chart_type,
        "title": title,
        "x": x,
        "y": y,
        "series": series,
        "reason": reason,
        "limit": limit,
        "chart_data_mode": chart_data_mode,
        "data": data or [],
        "columns": columns or {"x": x, "y": y, "series": series},
        "alternative_types": alternative_types or [],
    }


def _first_present(row: dict[str, Any], candidates: tuple[str, ...]) -> str | None:
    for column_name in candidates:
        if column_name in row:
            return column_name
    return None


def _first_numeric_metric(row: dict[str, Any]) -> str | None:
    for column_name in NUMERIC_METRIC_COLUMNS:
        if column_name in row and _is_number(row[column_name]):
            return column_name

    for column_name, value in row.items():
        normalized = column_name.lower()
        if normalized.endswith("_id") or normalized == "id":
            continue
        if _is_number(value):
            return column_name
    return None


def _numeric_columns(row: dict[str, Any]) -> list[str]:
    return [
        column_name
        for column_name, value in row.items()
        if _is_number(value) and not column_name.lower().endswith("_id")
    ]


def normalize_bar_data(rows: list[dict[str, Any]], x_col: str, y_col: str, limit: int | None = None) -> list[dict[str, Any]]:
    chart_limit = limit or DEFAULT_CHART_LIMIT
    data = []
    for row in rows[:chart_limit]:
        if x_col not in row or y_col not in row:
            continue
        y_value = _number_value(row.get(y_col))
        if y_value is None:
            continue
        data.append({"x": _json_value(row.get(x_col)), "y": y_value})
    return data


def normalize_line_data(rows: list[dict[str, Any]], x_col: str, y_col: str, limit: int | None = None) -> list[dict[str, Any]]:
    chart_limit = limit or DEFAULT_CHART_LIMIT
    data = []
    for row in rows[:chart_limit]:
        if x_col not in row or y_col not in row:
            continue
        y_value = _number_value(row.get(y_col))
        if y_value is None:
            continue
        data.append({"x": _json_value(row.get(x_col)), "y": y_value})
    return data


def normalize_pie_data(rows: list[dict[str, Any]], label_col: str, value_col: str, limit: int | None = None) -> list[dict[str, Any]]:
    chart_limit = limit or DEFAULT_CHART_LIMIT
    data = []
    for row in rows[:chart_limit]:
        if label_col not in row or value_col not in row:
            continue
        value = _number_value(row.get(value_col))
        if value is None or value < 0:
            continue
        data.append({"label": _json_value(row.get(label_col)), "value": value})

    total = sum(item["value"] for item in data)
    return data if total > 0 else []


def normalize_metrics_as_categories(row: dict[str, Any], metric_columns: list[str] | None = None, limit: int | None = None) -> list[dict[str, Any]]:
    chart_limit = limit or DEFAULT_CHART_LIMIT
    selected_columns = metric_columns or _numeric_columns(row)
    data = []
    for column_name in selected_columns[:chart_limit]:
        value = _number_value(row.get(column_name))
        if value is None:
            continue
        data.append({"x": column_name, "y": value})
    return data


def _normalize_metrics_as_pie(row: dict[str, Any], metric_columns: list[str], limit: int | None = None) -> list[dict[str, Any]]:
    chart_limit = limit or DEFAULT_CHART_LIMIT
    data = []
    for column_name in metric_columns[:chart_limit]:
        value = _number_value(row.get(column_name))
        if value is None or value < 0:
            continue
        data.append({"label": column_name, "value": value})

    total = sum(item["value"] for item in data)
    return data if total > 0 else []


def _not_enough_chart_data(reason: str) -> dict[str, Any]:
    return _empty_chart(reason)


def _revenue_is_zero(rows: list[dict[str, Any]]) -> bool:
    if not rows:
        return False
    revenue_values = [
        value
        for row in rows
        for column_name, value in row.items()
        if any(token in column_name.lower() for token in ("revenue", "amount", "doanh_thu", "doanhthu"))
        and _is_number(value)
    ]
    return bool(revenue_values) and all(value == 0 for value in revenue_values)


def _has_revenue_warning(warnings: list[str]) -> bool:
    return any("doanh thu" in warning.lower() or "revenue" in warning.lower() for warning in warnings)


def _metric_label(metric_column: str) -> str:
    if "revenue" in metric_column or "amount" in metric_column:
        return "doanh thu"
    if "view" in metric_column:
        return "số view"
    if "cart" in metric_column:
        return "số cart"
    if "purchase" in metric_column:
        return "số purchase"
    if "event" in metric_column:
        return "số event"
    return metric_column


def _ranking_title(dimension: str, metric: str) -> str:
    dimension_label = {
        "brand": "brand",
        "category_code": "category",
        "category_l1": "category",
        "category_l2": "category",
        "category_l3": "category",
        "product_id": "product",
        "product": "product",
        "event_type": "event type",
    }.get(dimension, dimension)
    return f"Top {dimension_label} theo {_metric_label(metric)}"


def recommend_chart(
    *,
    question: str,
    intent: str,
    rows: list[dict[str, Any]],
    row_count: int,
    generated_sql: str,
    table_candidates: list[str],
    used_tables: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    if intent in {"metadata_tables", "metadata_columns", "unsupported"}:
        return _empty_chart("Metadata hoặc câu hỏi cần làm rõ nên hiển thị dạng bảng/text.")

    if intent == "drilldown":
        return _empty_chart("Detail rows are better displayed as a table.")

    if not rows or row_count == 0:
        return _empty_chart("Không có dữ liệu đủ để đề xuất biểu đồ.")

    first_row = rows[0]

    if intent == "ranking":
        dimension = _first_present(first_row, DIMENSION_COLUMNS)
        metric = _first_numeric_metric(first_row)
        if dimension and metric:
            data = normalize_bar_data(rows, dimension, metric, limit=min(row_count, DEFAULT_CHART_LIMIT))
            if not data:
                return _not_enough_chart_data("Ranking result không tạo được chart_data numeric hợp lệ.")
            return _chart(
                chart_type="bar",
                title=_ranking_title(dimension, metric),
                x=dimension,
                y=metric,
                reason="Ranking có dimension và metric numeric, phù hợp với bar chart.",
                limit=min(row_count, DEFAULT_CHART_LIMIT),
                chart_data_mode="records",
                data=data,
                columns={"x": dimension, "y": metric, "series": None},
            )
        return _empty_chart("Ranking result thiếu dimension hoặc metric numeric rõ ràng.")

    if intent == "breakdown":
        dimension = _first_present(first_row, DIMENSION_COLUMNS)
        metric = _first_numeric_metric(first_row)
        if dimension and metric and row_count > 1:
            if row_count <= 6:
                pie_data = normalize_pie_data(rows, dimension, metric, limit=row_count)
                if pie_data:
                    return _chart(
                        chart_type="pie",
                        title=_ranking_title(dimension, metric),
                        x=dimension,
                        y=metric,
                        reason="Breakdown có ít nhóm dimension và metric không âm, phù hợp với pie chart.",
                        limit=row_count,
                        chart_data_mode="records",
                        data=pie_data,
                        columns={"label": dimension, "value": metric, "series": None},
                    )
            data = normalize_bar_data(rows, dimension, metric, limit=min(row_count, DEFAULT_CHART_LIMIT))
            if not data:
                return _not_enough_chart_data("Breakdown result không tạo được chart_data numeric hợp lệ.")
            return _chart(
                chart_type="bar",
                title=_ranking_title(dimension, metric),
                x=dimension,
                y=metric,
                reason="Breakdown có nhiều nhóm, bar chart là lựa chọn an toàn.",
                limit=min(row_count, DEFAULT_CHART_LIMIT),
                chart_data_mode="records",
                data=data,
                columns={"x": dimension, "y": metric, "series": None},
                alternative_types=["pie"] if row_count <= 6 else [],
            )

    if intent in {"metric_overview", "breakdown", "conversion_funnel"}:
        numeric_columns = _numeric_columns(first_row)
        if len(rows) == 1 and len(numeric_columns) > 1:
            event_metric_columns = [column for column in OVERVIEW_EVENT_METRICS if column in numeric_columns]
            selected_columns = event_metric_columns or numeric_columns
            if intent == "breakdown":
                pie_data = _normalize_metrics_as_pie(first_row, selected_columns, limit=DEFAULT_CHART_LIMIT)
                if pie_data:
                    return _chart(
                        chart_type="pie",
                        title="Cơ cấu các metric chính",
                        x="metric",
                        y="value",
                        reason="Breakdown một dòng có nhiều metric không âm và tổng lớn hơn 0, phù hợp với pie chart.",
                        limit=len(pie_data),
                        chart_data_mode="metrics_as_categories",
                        data=pie_data,
                        columns={"label": "metric", "value": "value", "series": None},
                        alternative_types=["bar"],
                    )
            data = normalize_metrics_as_categories(first_row, selected_columns, limit=DEFAULT_CHART_LIMIT)
            if not data:
                return _not_enough_chart_data("Overview result không tạo được chart_data numeric hợp lệ.")
            pie_data = _normalize_metrics_as_pie(first_row, selected_columns, limit=DEFAULT_CHART_LIMIT)
            return _chart(
                chart_type="bar",
                title="Tổng quan các metric chính",
                x="metric",
                y="value",
                reason="Một dòng có nhiều metric numeric nên có thể biểu diễn metric như category.",
                limit=len(data),
                chart_data_mode="metrics_as_categories",
                data=data,
                columns={"x": "metric", "y": "value", "series": None},
                alternative_types=["pie"] if pie_data else [],
            )
        if len(rows) == 1 and len(numeric_columns) == 1:
            return _empty_chart("Single KPI is better displayed as a card.")
        return _empty_chart("Overview result không có đủ nhiều metric numeric để đề xuất chart.")

    if intent == "revenue_sales":
        time_column = _first_present(first_row, TIME_COLUMNS)
        revenue_metric = _first_numeric_metric(first_row)
        dimension = _first_present(first_row, DIMENSION_COLUMNS)
        if len(rows) == 1 and _revenue_is_zero(rows):
            return _empty_chart("Doanh thu hiện bằng 0; KPI card phù hợp hơn chart.")
        if len(rows) == 1 and _has_revenue_warning(warnings):
            return _empty_chart("Kết quả revenue có caveat; KPI card phù hợp hơn chart.")
        if time_column and revenue_metric:
            data = normalize_line_data(rows, time_column, revenue_metric, limit=min(row_count, DEFAULT_CHART_LIMIT))
            if not data:
                return _not_enough_chart_data("Revenue trend result không tạo được chart_data numeric hợp lệ.")
            return _chart(
                chart_type="line",
                title=f"{_metric_label(revenue_metric).capitalize()} theo thời gian",
                x=time_column,
                y=revenue_metric,
                reason="Revenue result có cột thời gian và metric numeric, phù hợp với line chart.",
                limit=min(row_count, DEFAULT_CHART_LIMIT),
                chart_data_mode="records",
                data=data,
                columns={"x": time_column, "y": revenue_metric, "series": None},
            )
        if dimension and revenue_metric:
            data = normalize_bar_data(rows, dimension, revenue_metric, limit=min(row_count, DEFAULT_CHART_LIMIT))
            if not data:
                return _not_enough_chart_data("Revenue ranking result không tạo được chart_data numeric hợp lệ.")
            return _chart(
                chart_type="bar",
                title=f"{_ranking_title(dimension, revenue_metric)}",
                x=dimension,
                y=revenue_metric,
                reason="Revenue result có dimension và metric numeric, phù hợp với bar chart.",
                limit=min(row_count, DEFAULT_CHART_LIMIT),
                chart_data_mode="records",
                data=data,
                columns={"x": dimension, "y": revenue_metric, "series": None},
            )
        return _empty_chart("Single KPI is better displayed as a card.")

    if intent == "trend":
        time_column = _first_present(first_row, TIME_COLUMNS)
        metric = _first_numeric_metric(first_row)
        if time_column and metric:
            data = normalize_line_data(rows, time_column, metric, limit=min(row_count, DEFAULT_CHART_LIMIT))
            if not data:
                return _not_enough_chart_data("Trend result không tạo được chart_data numeric hợp lệ.")
            return _chart(
                chart_type="line",
                title=f"{_metric_label(metric).capitalize()} theo thời gian",
                x=time_column,
                y=metric,
                reason="Trend result có cột thời gian và metric numeric, phù hợp với line chart.",
                limit=min(row_count, DEFAULT_CHART_LIMIT),
                chart_data_mode="records",
                data=data,
                columns={"x": time_column, "y": metric, "series": None},
            )
        return _empty_chart("Trend result thiếu cột thời gian hoặc metric numeric rõ ràng.")

    dimension = _first_present(first_row, DIMENSION_COLUMNS)
    metric = _first_numeric_metric(first_row)
    if dimension and metric:
        if 1 < row_count <= 6:
            pie_data = normalize_pie_data(rows, dimension, metric, limit=row_count)
            if pie_data:
                return _chart(
                    chart_type="pie",
                    title=_ranking_title(dimension, metric),
                    x=dimension,
                    y=metric,
                    reason="Result có ít nhóm dimension và metric không âm, phù hợp với pie chart.",
                    limit=row_count,
                    chart_data_mode="records",
                    data=pie_data,
                    columns={"label": dimension, "value": metric, "series": None},
                )
        data = normalize_bar_data(rows, dimension, metric, limit=min(row_count, DEFAULT_CHART_LIMIT))
        if not data:
            return _not_enough_chart_data("Result không tạo được chart_data numeric hợp lệ.")
        return _chart(
            chart_type="bar",
            title=_ranking_title(dimension, metric),
            x=dimension,
            y=metric,
            reason="Result có dimension và metric numeric, bar chart là lựa chọn đơn giản.",
            limit=min(row_count, DEFAULT_CHART_LIMIT),
            chart_data_mode="records",
            data=data,
            columns={"x": dimension, "y": metric, "series": None},
        )

    return _empty_chart("Kết quả phù hợp hiển thị dạng bảng/text hơn chart.")
