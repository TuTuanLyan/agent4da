from services.metadata_service import build_schema_context, load_semantic_metadata
from services.security_service import validate_readonly_sql
from services.trino_service import execute_query_to_dicts, get_trino_connection


def get_schema_tool():
    metadata = load_semantic_metadata()
    return {
        "metadata": metadata,
        "schema_context": build_schema_context(metadata),
    }


def validate_sql_tool(sql):
    return validate_readonly_sql(sql)


def query_trino_tool(sql):
    return execute_query_to_dicts(
        get_trino_connection(),
        sql,
        raise_on_error=True,
    )
