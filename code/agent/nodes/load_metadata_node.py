from services.metadata_service import build_schema_context, load_semantic_metadata


def load_metadata_node(state):
    if state.get("schema_context"):
        return {}

    metadata = load_semantic_metadata()
    schema_context = build_schema_context(metadata)

    return {
        "schema_context": schema_context,
        "metadata_source": metadata.get("source"),
        "metadata_warning": metadata.get("warning"),
    }
