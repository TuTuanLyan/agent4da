"""Public error message helpers.

The frontend should never render raw Python tracebacks, dependency names from
unexpected imports, or Trino/JDBC internals. Routers log the original exception
and return these short messages to users.
"""

from __future__ import annotations


def public_error_message(exc: Exception, fallback: str) -> str:
    text = f"{exc.__class__.__name__}: {exc}"
    lowered = text.lower()

    if "groq_api_key" in lowered or "missing groq" in lowered:
        return "Groq is not configured. Add GROQ_API_KEY and restart the backend."
    if "401" in lowered and "airflow" in lowered:
        return "Airflow rejected the request. Check Airflow API credentials."
    if "airflow" in lowered:
        return "Airflow is unavailable. Check the Airflow service and credentials."
    if "iceberg_tables" in lowered or "semantic_table_catalog" in lowered:
        return (
            "Catalog metadata is not ready. Run the Gold and metadata pipelines, "
            "then retry."
        )
    if "trino" in lowered or "psqlexception" in lowered or "jdbc" in lowered:
        return "The data catalog is unavailable. Check Trino and Iceberg metadata."
    if "modulenotfounderror" in lowered or "importerror" in lowered:
        return "The backend runtime is missing a required dependency. Rebuild app-api."

    return fallback
