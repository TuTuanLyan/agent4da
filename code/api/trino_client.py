import os
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from dotenv import load_dotenv
import trino

load_dotenv()


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _column_name(description: Any) -> str:
    if isinstance(description, (tuple, list)):
        return str(description[0])
    return str(description.name)


def execute_query(sql: str) -> tuple[list[str], list[dict[str, Any]]]:
    conn = trino.dbapi.connect(
        host=os.getenv("TRINO_HOST", "trino"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user=os.getenv("TRINO_USER", "agent4da"),
        catalog=os.getenv("TRINO_CATALOG", "postgresql"),
        schema=os.getenv("TRINO_SCHEMA", "analytics_test"),
    )

    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        columns = [_column_name(column) for column in (cursor.description or [])]
        rows = [
            {column: _json_value(value) for column, value in zip(columns, row)}
            for row in cursor.fetchall()
        ]
        return columns, rows
    finally:
        cursor.close()
        conn.close()
