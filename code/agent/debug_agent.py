"""Minimal debug agent for semantic metadata -> Trino SQL.

This is intentionally linear and verbose. It prints every step so we can inspect
metadata retrieval, rule-based context selection, LLM SQL generation, SQL guard,
and optional Trino execution.
"""

import argparse
import os
import re
import sys
from collections import defaultdict


DEFAULT_QUESTION = "Top 5 thương hiệu có doanh thu cao nhất là gì?"
DEFAULT_MODEL = "llama-3.3-70b-versatile"
GROQ_BASE_URL = "https://api.groq.com/openai/v1"

ICEBERG_CATALOG = "iceberg"
ICEBERG_METADATA_SCHEMA = "metadata"

AGGREGATE_HINTS = {
    "top",
    "tổng",
    "theo",
    "cao",
    "cao nhất",
    "thấp",
    "thấp nhất",
    "trung bình",
    "bao nhiêu",
    "doanh thu",
    "tỷ lệ",
    "lượt",
}

IMPORTANT_COLUMNS = {
    "event_date",
    "sale_date",
    "brand",
    "category_l1",
    "category_l2",
    "category_l3",
    "product_id",
    "user_id",
    "session_id",
    "revenue",
    "total_revenue",
    "gross_amount",
    "view_count",
    "total_views",
    "purchase_count",
    "total_purchases",
    "cart_count",
    "total_carts",
    "conversion_rate",
    "cart_to_purchase_rate",
    "session_revenue",
    "session_duration_sec",
    "has_purchase",
}

METRIC_KEYWORDS = {
    "doanh thu": "total_revenue",
    "tổng doanh thu": "total_revenue",
    "tiền bán": "total_revenue",
    "tỷ lệ chuyển đổi": "conversion_rate",
    "conversion": "conversion_rate",
    "lượt mua": "purchase_count",
    "số lượt mua": "purchase_count",
    "giao dịch mua": "purchase_count",
    "lượt xem": "view_count",
    "số lượt xem": "view_count",
}

TABLE_BOOSTS = [
    (("brand", "thương hiệu", "hãng"), "gold.daily_brand_summary", 20),
    (("category", "danh mục", "ngành hàng"), "gold.daily_category_summary", 20),
    (("product", "sản phẩm"), "gold.daily_product_summary", 20),
    (("session", "phiên"), "gold.dim_session", 16),
    (("user", "khách hàng", "người dùng"), "gold.dim_user", 16),
    (("giao dịch", "purchase gần nhất", "mua gần nhất"), "gold.fact_sales", 18),
    (("ngày", "daily", "theo ngày", "doanh thu theo ngày"), "gold.daily_event_summary", 18),
]

FALLBACK_TABLES = [
    "gold.daily_event_summary",
    "gold.daily_product_summary",
    "gold.daily_brand_summary",
]


def print_section(title, body=None):
    print(f"\n=== {title} ===")
    if body is not None:
        print(body)


def normalize_text(value):
    value = "" if value is None else str(value)
    return re.sub(r"\s+", " ", value.lower()).strip()


def tokenize(text):
    return [
        token
        for token in re.split(r"[^\w]+", normalize_text(text), flags=re.UNICODE)
        if len(token) >= 3
    ]


def text_matches_question(question_text, candidate_text):
    candidate_text = normalize_text(candidate_text)
    if not candidate_text:
        return False
    if candidate_text in question_text:
        return True
    return any(token in candidate_text for token in tokenize(question_text))


def row_to_dict(cursor, row):
    names = [desc[0] for desc in cursor.description]
    return dict(zip(names, row))


def connect_trino():
    try:
        from trino.dbapi import connect
    except ImportError as exc:
        raise RuntimeError(
            "Missing Python package 'trino'. Install it with: pip install trino"
        ) from exc

    host = os.getenv("TRINO_HOST", "localhost")
    port = int(os.getenv("TRINO_PORT", "8082"))
    user = os.getenv("TRINO_USER", "agent4da")

    return connect(
        host=host,
        port=port,
        user=user,
        catalog=ICEBERG_CATALOG,
        schema=ICEBERG_METADATA_SCHEMA,
    )


def fetch_dicts(connection, sql):
    cursor = connection.cursor()
    try:
        cursor.execute(sql)
        return [row_to_dict(cursor, row) for row in cursor.fetchall()]
    finally:
        cursor.close()


def load_metadata(connection):
    tables = fetch_dicts(
        connection,
        """
        SELECT table_name, table_type, business_name, description, grain,
               is_agent_visible, recommended_for_agent
        FROM iceberg.metadata.table_catalog
        WHERE is_agent_visible = true
        """,
    )
    columns = fetch_dicts(
        connection,
        """
        SELECT table_name, column_name, data_type, business_name, description,
               is_dimension, is_metric, is_time_column, is_join_key, is_unique_key,
               agent_synonyms
        FROM iceberg.metadata.column_catalog
        """,
    )
    metrics = fetch_dicts(
        connection,
        """
        SELECT metric_name, business_name, description, formula_sql,
               base_table, default_time_column, aggregation_type, unit, example_question
        FROM iceberg.metadata.metric_catalog
        """,
    )
    joins = fetch_dicts(
        connection,
        """
        SELECT join_id, left_table, left_key, right_table, right_key,
               relationship_type, description
        FROM iceberg.metadata.join_catalog
        """,
    )
    return {"tables": tables, "columns": columns, "metrics": metrics, "joins": joins}


def score_metadata(question, metadata):
    question_text = normalize_text(question)
    scores = defaultdict(int)
    reasons = defaultdict(list)
    has_summary_match = False

    for table in metadata["tables"]:
        table_name = table["table_name"]
        table_text = " ".join(
            str(table.get(key) or "")
            for key in ["table_name", "business_name", "description", "grain"]
        )
        if text_matches_question(question_text, table_text):
            scores[table_name] += 5
            reasons[table_name].append("+5 table text match")
        if table.get("recommended_for_agent"):
            scores[table_name] += 3
            reasons[table_name].append("+3 recommended")
        if table.get("table_type") == "summary" and any(hint in question_text for hint in AGGREGATE_HINTS):
            scores[table_name] += 2
            reasons[table_name].append("+2 aggregate summary hint")

    for column in metadata["columns"]:
        table_name = column["table_name"]
        column_text = " ".join(
            str(column.get(key) or "")
            for key in ["column_name", "business_name", "description", "agent_synonyms"]
        )
        if text_matches_question(question_text, column_text):
            scores[table_name] += 8
            reasons[table_name].append(f"+8 column match {column['column_name']}")

    for metric in metadata["metrics"]:
        metric_text = " ".join(
            str(metric.get(key) or "")
            for key in ["metric_name", "business_name", "description", "example_question"]
        )
        if text_matches_question(question_text, metric_text):
            table_name = metric["base_table"]
            scores[table_name] += 8
            reasons[table_name].append(f"+8 metric match {metric['metric_name']}")

    for keywords, table_name, boost in TABLE_BOOSTS:
        if any(keyword in question_text for keyword in keywords):
            scores[table_name] += boost
            reasons[table_name].append(f"+{boost} keyword boost")

    for table in metadata["tables"]:
        table_name = table["table_name"]
        if table.get("table_type") == "summary" and scores[table_name] >= 10:
            has_summary_match = True

    if has_summary_match:
        for table in metadata["tables"]:
            table_name = table["table_name"]
            if table.get("table_type") == "fact":
                scores[table_name] -= 2
                reasons[table_name].append("-2 fact when summary matches")

    return scores, reasons


def select_tables(question, metadata, max_tables=3):
    scores, reasons = score_metadata(question, metadata)
    available = {table["table_name"] for table in metadata["tables"]}
    ranked = sorted(available, key=lambda name: (-scores[name], name))
    selected = [name for name in ranked if scores[name] > 0][:max_tables]

    if not selected or max(scores[name] for name in selected) < 5:
        selected = [name for name in FALLBACK_TABLES if name in available][:max_tables]

    return selected, scores, reasons


def column_match_score(question, column):
    question_text = normalize_text(question)
    text = " ".join(
        str(column.get(key) or "")
        for key in ["column_name", "business_name", "description", "agent_synonyms"]
    )
    score = 0
    if column["column_name"] in IMPORTANT_COLUMNS:
        score += 6
    if text_matches_question(question_text, text):
        score += 10
    if column.get("is_time_column"):
        score += 2
    if column.get("is_join_key"):
        score += 1
    return score


def select_columns(question, metadata, selected_tables, per_table_limit=12):
    grouped = defaultdict(list)
    for column in metadata["columns"]:
        if column["table_name"] in selected_tables:
            grouped[column["table_name"]].append(column)

    selected = {}
    for table_name in selected_tables:
        columns = grouped.get(table_name, [])
        if len(columns) <= per_table_limit:
            selected[table_name] = columns
            continue
        ranked = sorted(
            columns,
            key=lambda column: (
                -column_match_score(question, column),
                column["column_name"],
            ),
        )
        selected[table_name] = ranked[:per_table_limit]
    return selected


def select_metrics(question, metadata):
    question_text = normalize_text(question)
    selected = []
    seen = set()

    def add(metric):
        name = metric["metric_name"]
        if name not in seen:
            selected.append(metric)
            seen.add(name)

    for phrase, metric_name in METRIC_KEYWORDS.items():
        if phrase in question_text:
            for metric in metadata["metrics"]:
                if metric["metric_name"] == metric_name:
                    add(metric)

    for metric in metadata["metrics"]:
        metric_text = " ".join(
            str(metric.get(key) or "")
            for key in ["metric_name", "business_name", "description", "example_question"]
        )
        if text_matches_question(question_text, metric_text):
            add(metric)

    return selected


def select_joins(metadata, selected_tables):
    if len(selected_tables) <= 1:
        return []
    selected_set = set(selected_tables)
    return [
        join
        for join in metadata["joins"]
        if join["left_table"] in selected_set and join["right_table"] in selected_set
    ]


def selected_table_rows(metadata, selected_tables):
    by_name = {table["table_name"]: table for table in metadata["tables"]}
    return [by_name[name] for name in selected_tables if name in by_name]


def full_table_name(short_name):
    if short_name.startswith("iceberg."):
        return short_name
    return f"{ICEBERG_CATALOG}.{short_name}"


def compact_column_line(column):
    flags = []
    if column.get("is_time_column"):
        flags.append("time column")
    if column.get("is_dimension"):
        flags.append("dimension")
    if column.get("is_metric"):
        flags.append("metric")
    if column.get("is_join_key"):
        flags.append("join key")
    if column.get("is_unique_key"):
        flags.append("unique key")

    detail = ", ".join(flags) if flags else "column"
    line = (
        f"- {column['column_name']} {column['data_type']}: "
        f"{detail}. {column.get('description') or ''}".strip()
    )
    if column.get("agent_synonyms"):
        line += f" Synonyms: {column['agent_synonyms']}."
    return line


def build_schema_context(table_rows, selected_columns, selected_metrics, selected_joins):
    lines = [
        "You can use only these tables.",
        "",
    ]
    for table in table_rows:
        table_name = table["table_name"]
        lines.extend(
            [
                f"Table: {full_table_name(table_name)}",
                f"Purpose: {table.get('description') or table.get('business_name')}",
                f"Grain: {table.get('grain')}",
                "Columns:",
            ]
        )
        for column in selected_columns.get(table_name, []):
            lines.append(compact_column_line(column))
        lines.append("")

    if selected_metrics:
        lines.append("Metrics:")
        for metric in selected_metrics:
            lines.append(
                "- {metric_name}: {formula_sql}, base table {base_table}.".format(
                    metric_name=metric["metric_name"],
                    formula_sql=metric["formula_sql"],
                    base_table=metric["base_table"],
                )
            )
        lines.append("")

    if selected_joins:
        lines.append("Safe joins:")
        for join in selected_joins:
            lines.append(
                "- {left_table}.{left_key} -> {right_table}.{right_key}: {description}".format(
                    **join
                )
            )
        lines.append("")

    lines.extend(
        [
            "Rules:",
            "- Generate Trino SQL only.",
            "- Use SELECT only.",
            "- Do not invent tables or columns.",
            "- Use fully qualified table names with iceberg catalog, e.g. iceberg.gold.daily_brand_summary.",
            "- Prefer summary tables for aggregate questions.",
            "- Add LIMIT for detail queries.",
            "- Do not use SHOW TABLES because JDBC V0 may fail on Trino 481.",
            "- If table list is needed, query iceberg.metadata.table_catalog.",
        ]
    )
    return "\n".join(lines)


def require_groq_api_key():
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise RuntimeError("Missing GROQ_API_KEY. Export it before running the debug agent.")
    return api_key


def generate_sql(question, schema_context):
    try:
        from openai import OpenAI
    except ImportError as exc:
        raise RuntimeError(
            "Missing Python package 'openai'. Install it with: pip install openai"
        ) from exc

    client = OpenAI(api_key=require_groq_api_key(), base_url=GROQ_BASE_URL)
    model = os.getenv("AGENT_MODEL", DEFAULT_MODEL)
    system_prompt = (
        "You are a Trino SQL analyst for an ecommerce Iceberg lakehouse.\n"
        "Generate one valid Trino SELECT query for the user's question.\n"
        "Use only the schema context provided.\n"
        "Do not invent table or column names.\n"
        "Return only SQL inside a code block or plain SQL.\n"
        "No explanation."
    )
    user_prompt = f"Schema context:\n{schema_context}\n\nUser question:\n{question}"
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0,
    )
    raw = response.choices[0].message.content or ""
    return raw, extract_sql(raw)


def extract_sql(raw_response):
    text = raw_response.strip()
    fenced = re.search(r"```(?:sql)?\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL)
    if fenced:
        text = fenced.group(1).strip()
    return text.rstrip(";").strip()


def referenced_tables(sql):
    pattern = re.compile(
        r"\b(?:from|join)\s+([a-zA-Z_][\w]*(?:\.[a-zA-Z_][\w]*){1,2})",
        flags=re.IGNORECASE,
    )
    return [match.group(1) for match in pattern.finditer(sql)]


def guard_sql(sql, selected_tables, allow_metadata_tables=False):
    normalized = normalize_text(sql)
    if not normalized.startswith(("select", "with")):
        return False, "SQL must start with SELECT or WITH."

    blocked = r"\b(insert|update|delete|drop|alter|create|truncate|merge|call)\b"
    if re.search(blocked, normalized, flags=re.IGNORECASE):
        return False, "SQL contains a blocked write/admin keyword."

    if re.search(r";\s*\S", sql):
        return False, "SQL contains multiple statements."

    allowed = {full_table_name(name) for name in selected_tables}
    if allow_metadata_tables:
        allowed.update(
            {
                "iceberg.metadata.table_catalog",
                "iceberg.metadata.column_catalog",
                "iceberg.metadata.metric_catalog",
                "iceberg.metadata.join_catalog",
            }
        )

    refs = referenced_tables(sql)
    bad_refs = []
    for ref in refs:
        full_ref = ref if ref.startswith("iceberg.") else f"{ICEBERG_CATALOG}.{ref}"
        if full_ref not in allowed:
            bad_refs.append(ref)

    if bad_refs:
        return False, f"SQL references tables outside selected context: {bad_refs}"

    return True, "PASS"


def execute_sql(connection, sql, max_rows):
    cursor = connection.cursor()
    try:
        cursor.execute(sql)
        rows = cursor.fetchmany(max_rows)
        columns = [desc[0] for desc in cursor.description]
        return columns, [dict(zip(columns, row)) for row in rows]
    finally:
        cursor.close()


def summarize_answer(question, sql, rows):
    if os.getenv("AGENT_SUMMARIZE", "false").lower() not in {"1", "true", "yes"}:
        return "Query executed. See rows above."

    try:
        from openai import OpenAI
    except ImportError as exc:
        raise RuntimeError(
            "Missing Python package 'openai'. Install it with: pip install openai"
        ) from exc

    client = OpenAI(api_key=require_groq_api_key(), base_url=GROQ_BASE_URL)
    model = os.getenv("AGENT_MODEL", DEFAULT_MODEL)
    response = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": "Answer briefly in Vietnamese using only the SQL result rows.",
            },
            {
                "role": "user",
                "content": f"Question: {question}\nSQL: {sql}\nRows: {rows}",
            },
        ],
        temperature=0,
    )
    return (response.choices[0].message.content or "").strip()


def print_selected_columns(selected_columns):
    for table_name, columns in selected_columns.items():
        print(f"{table_name}:")
        for column in columns:
            flags = []
            if column.get("is_dimension"):
                flags.append("dimension")
            if column.get("is_metric"):
                flags.append("metric")
            if column.get("is_time_column"):
                flags.append("time")
            if column.get("is_join_key"):
                flags.append("join")
            print(f"  - {column['column_name']} ({column['data_type']}; {', '.join(flags) or 'column'})")


def print_rows(rows):
    if not rows:
        print("[]")
        return
    for index, row in enumerate(rows, start=1):
        print(f"{index}. {row}")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Debug semantic metadata Agent -> Trino SQL.")
    parser.add_argument("question", nargs="*", help="User question.")
    parser.add_argument("--no-execute", action="store_true", help="Generate SQL but skip final Trino execution.")
    parser.add_argument("--max-rows", type=int, default=20, help="Maximum result rows to print.")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    question = " ".join(args.question).strip() or DEFAULT_QUESTION

    print_section("USER QUESTION", question)

    connection = connect_trino()
    try:
        metadata = load_metadata(connection)
        print_section(
            "METADATA LOADED",
            (
                f"tables={len(metadata['tables'])}, "
                f"columns={len(metadata['columns'])}, "
                f"metrics={len(metadata['metrics'])}, "
                f"joins={len(metadata['joins'])}"
            ),
        )

        selected_tables, table_scores, score_reasons = select_tables(question, metadata)
        selected_columns = select_columns(question, metadata, selected_tables)
        selected_metrics = select_metrics(question, metadata)
        selected_joins = select_joins(metadata, selected_tables)
        table_rows = selected_table_rows(metadata, selected_tables)
        schema_context = build_schema_context(
            table_rows,
            selected_columns,
            selected_metrics,
            selected_joins,
        )

        print_section("SELECTED TABLE SCORES")
        for table_name in sorted(table_scores, key=lambda name: (-table_scores[name], name)):
            if table_scores[table_name] != 0:
                print(f"{table_name}: {table_scores[table_name]} | {'; '.join(score_reasons[table_name])}")

        print_section("SELECTED TABLES")
        for table_name in selected_tables:
            print(f"- {table_name} -> {full_table_name(table_name)}")

        print_section("SELECTED COLUMNS")
        print_selected_columns(selected_columns)

        print_section("SELECTED METRICS")
        if selected_metrics:
            for metric in selected_metrics:
                print(f"- {metric['metric_name']}: {metric['formula_sql']} on {metric['base_table']}")
        else:
            print("[]")

        print_section("SELECTED JOINS")
        if selected_joins:
            for join in selected_joins:
                print(
                    "- {left_table}.{left_key} -> {right_table}.{right_key} ({relationship_type})".format(
                        **join
                    )
                )
        else:
            print("[]")

        print_section("COMPACT SCHEMA CONTEXT", schema_context)

        raw_response, sql = generate_sql(question, schema_context)
        print_section("LLM RAW RESPONSE", raw_response)
        print_section("GENERATED SQL", sql)

        question_text = normalize_text(question)
        allow_metadata_tables = any(
            phrase in question_text
            for phrase in ["metadata", "table list", "bảng nào", "danh sách bảng", "có bảng"]
        )
        ok, reason = guard_sql(sql, selected_tables, allow_metadata_tables)
        print_section("SQL GUARD", "PASS" if ok else f"FAIL: {reason}")
        if not ok:
            print_section("COPY THIS SQL", sql + ";")
            return 2

        result_rows = []
        if args.no_execute:
            print_section("TRINO RESULT SAMPLE", "Skipped because --no-execute was set.")
            final_answer = "SQL generated. Execution skipped."
        else:
            try:
                result_columns, result_rows = execute_sql(connection, sql, args.max_rows)
                print_section("TRINO RESULT SAMPLE")
                print(f"columns: {result_columns}")
                print(f"row_count_fetched: {len(result_rows)}")
                print("rows:")
                print_rows(result_rows)
                final_answer = summarize_answer(question, sql, result_rows)
            except Exception as exc:
                print_section("TRINO QUERY ERROR", f"{type(exc).__name__}: {exc}")
                print_section("COPY THIS SQL", sql + ";")
                return 3

        print_section("COPY THIS SQL", sql + ";")
        print_section("FINAL ANSWER", final_answer)
        return 0
    finally:
        connection.close()


if __name__ == "__main__":
    sys.exit(main())
