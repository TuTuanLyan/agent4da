import re
import unicodedata
import os


DESTRUCTIVE_QUESTION_TERMS = [
    "drop",
    "delete",
    "truncate",
    "update",
    "insert",
    "alter",
    "create",
    "merge",
    "grant",
    "revoke",
    "xoa bang",
    "xoa du lieu",
    "xoa het",
    "xóa bảng",
    "xóa dữ liệu",
    "xóa hết",
    "cap nhat du lieu",
    "cập nhật dữ liệu",
    "sua du lieu",
    "sửa dữ liệu",
    "them du lieu",
    "thêm dữ liệu",
]

PROMPT_INJECTION_TERMS = [
    "ignore previous",
    "ignore all previous",
    "ignore instructions",
    "bypass",
    "jailbreak",
    "system prompt",
    "developer message",
    "khong can truy van",
    "không cần truy vấn",
    "khong can query",
    "không cần query",
    "tra loi ngay",
    "trả lời ngay",
    "answer directly",
    "answer immediately",
]

FORBIDDEN_SQL_KEYWORDS = [
    "INSERT",
    "UPDATE",
    "DELETE",
    "DROP",
    "ALTER",
    "TRUNCATE",
    "CREATE",
    "MERGE",
    "CALL",
    "GRANT",
    "REVOKE",
    "EXECUTE",
]


def normalize_text(text):
    text = unicodedata.normalize("NFD", (text or "").lower())
    return "".join(
        char for char in text
        if unicodedata.category(char) != "Mn"
    )


def detect_question_risk(question):
    normalized = normalize_text(question)

    for term in DESTRUCTIVE_QUESTION_TERMS:
        if normalize_text(term) in normalized:
            return {
                "allowed": False,
                "reason": "Câu hỏi có ý định thay đổi hoặc xóa dữ liệu.",
                "category": "destructive_intent",
                "matched": term,
            }

    for term in PROMPT_INJECTION_TERMS:
        if normalize_text(term) in normalized:
            return {
                "allowed": False,
                "reason": "Câu hỏi yêu cầu bỏ qua quy trình truy vấn hoặc guardrail.",
                "category": "prompt_injection",
                "matched": term,
            }

    return {
        "allowed": True,
        "reason": "Câu hỏi hợp lệ cho truy vấn chỉ đọc.",
        "category": "safe",
        "matched": None,
    }


def clean_sql(sql):
    sql = (sql or "").strip()
    sql = re.sub(r"^\s*```(?:sql)?\s*", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\s*```\s*$", "", sql)
    sql = sql.strip()

    if sql.endswith(";"):
        sql = sql[:-1].strip()

    return sql


def strip_comments_and_strings(sql):
    without_comments = re.sub(r"--.*?$", " ", sql, flags=re.MULTILINE)
    without_comments = re.sub(r"/\*.*?\*/", " ", without_comments, flags=re.DOTALL)
    without_strings = re.sub(r"'(?:''|[^'])*'", "''", without_comments)
    return re.sub(r'"(?:""|[^"])*"', '""', without_strings)


def has_multiple_statements(sql):
    return ";" in sql


def is_allowed_readonly_query(sql):
    normalized = re.sub(r"\s+", " ", sql).strip()
    upper_sql = normalized.upper()
    return upper_sql.startswith("SELECT ") or upper_sql.startswith("WITH ")


def find_forbidden_keyword(sql):
    searchable_sql = strip_comments_and_strings(sql).upper()

    for keyword in FORBIDDEN_SQL_KEYWORDS:
        if re.search(rf"\b{keyword}\b", searchable_sql):
            return keyword

    return None


def validate_readonly_sql(sql):
    cleaned = clean_sql(sql)

    if not cleaned:
        return {
            "allowed": False,
            "sql": "",
            "reason": "SQL rỗng.",
            "category": "empty_sql",
        }

    if has_multiple_statements(cleaned):
        return {
            "allowed": False,
            "sql": "",
            "reason": "Chỉ cho phép một câu SQL duy nhất.",
            "category": "multiple_statements",
        }

    if not is_allowed_readonly_query(cleaned):
        return {
            "allowed": False,
            "sql": "",
            "reason": "Chỉ cho phép SQL dạng SELECT hoặc WITH ... SELECT.",
            "category": "not_select",
        }

    forbidden_keyword = find_forbidden_keyword(cleaned)
    if forbidden_keyword:
        return {
            "allowed": False,
            "sql": "",
            "reason": f"SQL chứa keyword không an toàn: {forbidden_keyword}.",
            "category": "forbidden_keyword",
        }

    return {
        "allowed": True,
        "sql": cleaned,
        "reason": "SQL hợp lệ và chỉ đọc.",
        "category": "safe",
    }


def has_limit_clause(sql):
    searchable_sql = strip_comments_and_strings(sql).upper()
    return re.search(r"\bLIMIT\b", searchable_sql) is not None


def add_default_limit(sql, default_limit=None):
    default_limit = int(default_limit or os.getenv("AGENT_DEFAULT_SQL_LIMIT", "100"))
    if default_limit <= 0 or has_limit_clause(sql):
        return sql

    return f"{clean_sql(sql)}\nLIMIT {default_limit}"
