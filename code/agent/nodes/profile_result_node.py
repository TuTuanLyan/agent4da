from decimal import Decimal


TIME_KEYWORDS = ["date", "time", "month", "year", "day"]


def is_numeric_value(value):
    return isinstance(value, (int, float, Decimal)) and not isinstance(value, bool)


def is_time_column(column_name):
    lower_name = str(column_name).lower()
    return any(keyword in lower_name for keyword in TIME_KEYWORDS)


def first_non_null_value(rows, column_name):
    for row in rows:
        value = row.get(column_name)
        if value is not None:
            return value
    return None


def empty_profile():
    return {
        "row_count": 0,
        "columns": [],
        "numeric_columns": [],
        "categorical_columns": [],
        "time_columns": []
    }


def profile_result_node(state):
    rows = state.get("query_result") or []

    if not rows:
        return {
            "result_profile": empty_profile()
        }

    first_row = rows[0]
    columns = list(first_row.keys())

    numeric_columns = []
    time_columns = []

    for column in columns:
        if is_time_column(column):
            time_columns.append(column)

        value = first_non_null_value(rows, column)
        if is_numeric_value(value):
            numeric_columns.append(column)

    categorical_columns = [
        column
        for column in columns
        if column not in numeric_columns and column not in time_columns
    ]

    return {
        "result_profile": {
            "row_count": len(rows),
            "columns": columns,
            "numeric_columns": numeric_columns,
            "categorical_columns": categorical_columns,
            "time_columns": time_columns
        }
    }
