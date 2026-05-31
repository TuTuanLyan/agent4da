"""Session analytical spec + delta-merge engine for the v2 agent.

The "Active Query Spec" is the resolved ``intent_result`` of the last successful
analytical turn (same shape NLU emits and the SQL builder consumes). A follow-up
turn is resolved as a typed *patch* applied to that spec, so refinements compose
across many turns:

    T1 ranking brand/views/2020
    T2 + filter brand NOT IN unknown
    T3 limit -> 5
    T4 metric -> revenue
    T5 time -> 2021

``merge_spec`` is pure and fully unit-testable (no Trino/LLM). Every patch should
already be validated against semantic metadata by the caller; this module only
applies set/add/remove semantics and keeps the spec internally consistent
(re-deriving table candidates when the dimension/metric changes).
"""

from __future__ import annotations

import copy
from typing import Any

from .nlu import _table_candidates  # internal but stable; spec lives beside nlu

# Canonical spec keys (a superset that round-trips through the SQL builder).
_SPEC_KEYS = (
    "intent",
    "dimension",
    "metric",
    "analysis_type",
    "time_range",
    "time_grain",
    "applied_time_filter",
    "filters",
    "comparison_entities",
    "sort_direction",
    "limit",
    "table_candidates",
    "table_name",
    "needs_metadata",
    "extracted_entities",
    "nlu_confidence",
)

_DEFAULTS: dict[str, Any] = {
    "intent": None,
    "dimension": None,
    "metric": None,
    "analysis_type": None,
    "time_range": None,
    "time_grain": None,
    "applied_time_filter": None,
    "filters": [],
    "comparison_entities": [],
    "sort_direction": None,
    "limit": 10,
    "table_candidates": [],
    "table_name": None,
    "needs_metadata": True,
    "extracted_entities": {},
    "nlu_confidence": "high",
}

# Intents whose spec is meaningful to carry into a follow-up.
DIMENSIONAL_INTENTS = {
    "ranking",
    "comparison",
    "breakdown",
    "revenue_sales",
    "trend",
    "metric_overview",
    "drilldown",
    "conversion_funnel",
}

_INCLUSION_OPERATORS = {"in", "=", "eq", "include"}
_EXCLUSION_OPERATORS = {"not_in", "!=", "<>", "ne", "exclude", "not"}
_NUMERIC_OPERATORS = {">", ">=", "<", "<=", "between"}


def canonical_spec(source: dict[str, Any] | None) -> dict[str, Any]:
    """Build a complete spec dict from any source that carries the same keys
    (an NLU ``intent_result``, a graph response, or a persisted ``agent_trace``)."""
    source = source or {}
    spec: dict[str, Any] = {}
    for key in _SPEC_KEYS:
        if key in source and source[key] is not None:
            spec[key] = copy.deepcopy(source[key])
        else:
            spec[key] = copy.deepcopy(_DEFAULTS[key])
    # metrics live inside extracted_entities; keep them addressable.
    metrics = (source.get("extracted_entities") or {}).get("metrics") if source else None
    if metrics:
        spec["extracted_entities"] = dict(spec.get("extracted_entities") or {})
        spec["extracted_entities"]["metrics"] = list(metrics)
    return spec


def _clamp_limit(value: Any, fallback: int) -> int:
    try:
        return max(1, min(int(value), 100))
    except (TypeError, ValueError):
        return fallback


def _same_field(filter_a: dict[str, Any], field: str) -> bool:
    return str(filter_a.get("field")) == str(field)


def _apply_add_filter(filters: list[dict[str, Any]], new_filter: dict[str, Any]) -> list[dict[str, Any]]:
    field = new_filter.get("field")
    operator = (new_filter.get("operator") or "").lower()
    if not field:
        return filters
    out = list(filters)

    if operator in _EXCLUSION_OPERATORS:
        # Accumulate exclusions on the same field (bỏ qua A, rồi bỏ qua B).
        for existing in out:
            if _same_field(existing, field) and (existing.get("operator") or "").lower() in _EXCLUSION_OPERATORS:
                values = list(existing.get("values") or [])
                for value in new_filter.get("values") or []:
                    if value not in values:
                        values.append(value)
                existing["values"] = values
                return out
        out.append(dict(new_filter))
        return out

    # Inclusion / numeric: replace any existing filter with the same field+operator
    # (so "chỉ 2021" overrides "chỉ 2020"), but keep different operators on the same
    # field (e.g. an IN plus a NOT IN).
    out = [
        existing
        for existing in out
        if not (_same_field(existing, field) and (existing.get("operator") or "").lower() == operator)
    ]
    out.append(dict(new_filter))
    return out


def merge_spec(active_spec: dict[str, Any] | None, patch: dict[str, Any] | None) -> dict[str, Any]:
    """Apply a typed patch to the active spec and return a new, consistent spec.

    Patch shape (all keys optional):
      - ``set``:           {spec_field: value}             (replace)
      - ``add_filters``:   [filter, ...]                   (exclusions accumulate;
                                                            inclusions/numerics replace
                                                            same field+operator)
      - ``remove_filter_fields``: [field, ...]             (drop filters on these fields)
      - ``add_metrics``:   [metric, ...]                   (multi-metric output)
    """
    spec = canonical_spec(active_spec)
    patch = patch or {}

    set_fields = patch.get("set") or {}
    dimension_changed = False
    metric_changed = False
    for key, value in set_fields.items():
        if key not in _SPEC_KEYS:
            continue
        if key == "limit":
            spec["limit"] = _clamp_limit(value, spec["limit"])
            continue
        if key == "dimension":
            dimension_changed = dimension_changed or value != spec.get("dimension")
        if key == "metric":
            metric_changed = metric_changed or value != spec.get("metric")
        spec[key] = copy.deepcopy(value)

    # A metric *switch* must also reset the metric list the SQL builder reads,
    # otherwise the old aggregate column would persist.
    if "metric" in set_fields and set_fields.get("metric"):
        extracted = dict(spec.get("extracted_entities") or {})
        extracted["metrics"] = [set_fields["metric"]]
        spec["extracted_entities"] = extracted

    add_metrics = patch.get("add_metrics") or []
    if add_metrics:
        extracted = dict(spec.get("extracted_entities") or {})
        metrics = list(extracted.get("metrics") or ([spec["metric"]] if spec.get("metric") else []))
        for metric in add_metrics:
            if metric and metric not in metrics:
                metrics.append(metric)
        extracted["metrics"] = metrics
        spec["extracted_entities"] = extracted
        if not spec.get("metric") and metrics:
            spec["metric"] = metrics[0]
        metric_changed = True

    remove_fields = {str(field) for field in (patch.get("remove_filter_fields") or [])}
    if remove_fields:
        spec["filters"] = [f for f in spec.get("filters") or [] if str(f.get("field")) not in remove_fields]

    for new_filter in patch.get("add_filters") or []:
        spec["filters"] = _apply_add_filter(spec.get("filters") or [], new_filter)

    # Re-derive table candidates when the analytical shape changed.
    if dimension_changed or metric_changed:
        spec["table_candidates"] = _table_candidates(
            spec.get("dimension"),
            spec.get("metric"),
            spec.get("intent") or "ranking",
            spec.get("time_grain"),
        )
        # A dimension switch invalidates entity comparisons from the old shape.
        if dimension_changed:
            spec["comparison_entities"] = []

    spec["limit"] = _clamp_limit(spec.get("limit"), 10)
    return spec


def render_spec_question(spec: dict[str, Any]) -> str:
    """A compact natural-language restatement of a spec.

    Used to ground the LLM SQL fallback and for human-readable traces; the
    deterministic SQL builder works straight off the spec, so this never has to
    be perfectly parseable.
    """
    dimension = spec.get("dimension") or ""
    metric = spec.get("metric") or "total_events"
    parts: list[str] = []
    direction = "thấp nhất" if spec.get("sort_direction") == "asc" else "cao nhất"
    if dimension:
        parts.append(f"{dimension} theo {metric} ({direction})")
    else:
        parts.append(f"{metric}")
    time_range = spec.get("time_range")
    if isinstance(time_range, dict) and time_range.get("start"):
        if time_range.get("start") == time_range.get("end"):
            parts.append(f"ngày {time_range['start']}")
        else:
            parts.append(f"từ {time_range.get('start')} đến {time_range.get('end')}")
    for spec_filter in spec.get("filters") or []:
        operator = (spec_filter.get("operator") or "").lower()
        values = ", ".join(str(v) for v in spec_filter.get("values") or [])
        if not values:
            continue
        if operator in _EXCLUSION_OPERATORS:
            parts.append(f"bỏ qua {spec_filter.get('field')} {values}")
        elif operator in _INCLUSION_OPERATORS:
            parts.append(f"chỉ {spec_filter.get('field')} {values}")
    limit = spec.get("limit")
    if limit:
        parts.append(f"top {limit}")
    return "; ".join(parts)
