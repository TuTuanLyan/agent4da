from services.metadata_service import build_schema_context, load_semantic_metadata


def load_metadata_node(state):
    if state.get("schema_context"):
        return {}

    try:
        metadata = load_semantic_metadata()
    except Exception as exc:
        message = f"Failed to load semantic metadata: {type(exc).__name__}: {exc}"
        print(f"[Metadata] {message}", flush=True)
        return {
            "schema_context": "",
            "error": message,
        }

    tables = metadata.get("tables", [])
    if not tables:
        message = (
            "Semantic metadata is empty: no agent-visible tables found in "
            "iceberg.metadata.semantic_table_catalog. Run the Gold metadata DAG "
            "to populate it."
        )
        print(f"[Metadata] {message}", flush=True)
        return {
            "schema_context": "",
            "error": message,
        }

    schema_context = build_schema_context(metadata)

    return {
        "schema_context": schema_context,
    }
