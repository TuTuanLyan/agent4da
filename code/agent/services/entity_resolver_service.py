import re
import unicodedata
from difflib import SequenceMatcher
from functools import lru_cache

from services.trino_service import execute_query_to_dicts, get_trino_connection


ENTITY_CONFIGS = [
    {
        "dimension": "brand",
        "table": "gold.daily_brand_summary",
        "column": "brand",
        "anchors": [
            ("cua",),
            ("brand",),
            ("hang",),
            ("nhan", "hang"),
            ("thuong", "hieu"),
        ],
    },
    {
        "dimension": "category_l1",
        "table": "gold.daily_category_summary",
        "column": "category_l1",
        "anchors": [
            ("cua",),
            ("category",),
            ("danh", "muc"),
            ("nganh", "hang"),
        ],
    },
    {
        "dimension": "category_l2",
        "table": "gold.daily_category_summary",
        "column": "category_l2",
        "anchors": [
            ("cua",),
            ("category",),
            ("danh", "muc"),
            ("nganh", "hang"),
        ],
    },
    {
        "dimension": "category_l3",
        "table": "gold.daily_category_summary",
        "column": "category_l3",
        "anchors": [
            ("cua",),
            ("category",),
            ("danh", "muc"),
            ("nganh", "hang"),
        ],
    },
]


STOPWORDS = {
    "ai",
    "bao",
    "bang",
    "cao",
    "cho",
    "co",
    "con",
    "cua",
    "doanh",
    "duoc",
    "giu",
    "hang",
    "hay",
    "hieu",
    "la",
    "loc",
    "luot",
    "mua",
    "nam",
    "nao",
    "nhan",
    "nhat",
    "nhieu",
    "ngay",
    "san",
    "sap",
    "theo",
    "thang",
    "thu",
    "thuong",
    "top",
    "tong",
    "trong",
    "vao",
    "ve",
    "xem",
    "year",
    "month",
    "date",
    "revenue",
    "sales",
    "view",
    "views",
}


def normalize_text(text):
    text = unicodedata.normalize("NFD", (text or "").lower())
    normalized = "".join(
        char for char in text
        if unicodedata.category(char) != "Mn"
    )
    normalized = normalized.replace("đ", "d")
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return " ".join(normalized.split())


def tokenize(text):
    return normalize_text(text).split()


def anchor_matches(tokens, start, anchor):
    return tuple(tokens[start:start + len(anchor)]) == tuple(anchor)


def extract_anchor_candidates(question, anchors, max_tokens=3):
    tokens = tokenize(question)
    candidates = []

    for index in range(len(tokens)):
        for anchor in anchors:
            if not anchor_matches(tokens, index, anchor):
                continue

            start = index + len(anchor)
            words = []
            for token in tokens[start:start + max_tokens]:
                if token in STOPWORDS or token.isdigit():
                    break
                words.append(token)
                candidates.append(" ".join(words))

    return list(dict.fromkeys(candidates))


def levenshtein_distance(left, right):
    if left == right:
        return 0
    if not left:
        return len(right)
    if not right:
        return len(left)

    previous = list(range(len(right) + 1))
    for i, left_char in enumerate(left, 1):
        current = [i]
        for j, right_char in enumerate(right, 1):
            insert_cost = current[j - 1] + 1
            delete_cost = previous[j] + 1
            replace_cost = previous[j - 1] + (left_char != right_char)
            current.append(min(insert_cost, delete_cost, replace_cost))
        previous = current
    return previous[-1]


def escape_sql_literal(value):
    return str(value).replace("'", "''")


@lru_cache(maxsize=16)
def load_entity_values(table, column):
    connection = get_trino_connection(catalog="iceberg", schema="gold")
    rows = execute_query_to_dicts(
        connection,
        f"""
        SELECT DISTINCT {column} AS value
        FROM {table}
        WHERE {column} IS NOT NULL
          AND lower(trim(CAST({column} AS varchar))) <> 'unknown'
        ORDER BY {column}
        """,
        raise_on_error=True,
    )
    values = []
    for row in rows:
        value = row.get("value")
        normalized = normalize_text(value)
        if normalized:
            values.append({"value": value, "normalized": normalized})
    return values


def score_candidate(candidate, entity):
    candidate = normalize_text(candidate)
    value = entity["normalized"]
    if not candidate or not value:
        return None

    distance = levenshtein_distance(candidate, value)
    ratio = SequenceMatcher(None, candidate, value).ratio()
    max_len = max(len(candidate), len(value))
    distance_score = 1 - (distance / max_len)
    score = max(ratio, distance_score)

    if candidate == value:
        match_type = "exact"
    elif len(candidate) >= 4 and distance <= 2 and ratio >= 0.82:
        match_type = "fuzzy"
    elif len(candidate) >= 5 and distance <= 2 and score >= 0.78:
        match_type = "fuzzy"
    else:
        return None

    return {
        "score": round(score, 4),
        "distance": distance,
        "match_type": match_type,
    }


def best_match_for_candidates(candidates, values):
    best = None
    for candidate in candidates:
        for entity in values:
            scored = score_candidate(candidate, entity)
            if not scored:
                continue
            match = {
                **scored,
                "input": candidate,
                "resolved_value": entity["value"],
            }
            if best is None or (match["score"], -match["distance"]) > (best["score"], -best["distance"]):
                best = match
    return best


def resolve_entities(question):
    resolved = []

    for config in ENTITY_CONFIGS:
        candidates = extract_anchor_candidates(question, config["anchors"])
        if not candidates:
            continue

        values = load_entity_values(config["table"], config["column"])
        match = best_match_for_candidates(candidates, values)
        if not match:
            continue

        value = str(match["resolved_value"])
        column = config["column"]
        resolved.append(
            {
                "dimension": config["dimension"],
                "table": config["table"],
                "column": column,
                "input": match["input"],
                "resolved_value": value,
                "confidence": match["score"],
                "distance": match["distance"],
                "match_type": match["match_type"],
                "sql_predicate": (
                    f"lower(trim({column})) = lower(trim('{escape_sql_literal(value)}'))"
                ),
            }
        )

    return resolved
