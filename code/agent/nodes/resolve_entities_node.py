from services.entity_resolver_service import resolve_entities


def resolve_entities_node(state):
    try:
        resolved_entities = resolve_entities(state.get("user_question") or "")
        return {
            "resolved_entities": resolved_entities,
            "entity_resolution_warning": None,
        }
    except Exception as exc:
        return {
            "resolved_entities": [],
            "entity_resolution_warning": f"{type(exc).__name__}: {exc}",
        }
