import os

_CONNECTIONS = {}


def _register_query_id(run_id, cursor):
    if not run_id:
        return
    query_id = getattr(cursor, "query_id", None)
    if not query_id:
        stats = getattr(cursor, "stats", None)
        if isinstance(stats, dict):
            query_id = stats.get("queryId") or stats.get("query_id")
    if not query_id:
        return
    try:
        from agent import cancellation

        cancellation.set_trino_query_id(str(run_id), str(query_id))
    except Exception:
        # CLI/debug usage does not import the web backend package.
        return


def connect_to_trino(host, port, user, catalog, schema):
    try:
        from trino.dbapi import connect

        connection = connect(
            host=host,
            port=port,
            user=user,
            catalog=catalog,
            schema=schema,
            session_properties={"query_max_execution_time": "30s"},
        )
        print("[Trino] Connection to Trino established successfully.")
        return connection
    except Exception as e:
        print(f"[Trino] Failed to connect to Trino: {e}")
        return None


def get_trino_connection(catalog="iceberg", schema="metadata"):
    host = os.getenv("TRINO_HOST", "localhost")
    port = int(os.getenv("TRINO_PORT", "8082"))
    user = os.getenv("TRINO_USER", "agent4da")
    key = (host, port, user, catalog, schema)

    if key not in _CONNECTIONS:
        _CONNECTIONS[key] = connect_to_trino(
            host=host,
            port=port,
            user=user,
            catalog=catalog,
            schema=schema,
        )

    return _CONNECTIONS[key]


def execute_query(connection, query):
    if connection is None:
        print("[Trino] Cannot execute query because connection is not available.")
        return None

    cursor = None
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        return results
    except Exception as e:
        print(f"[Trino] Failed to execute query: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
    
def row_to_dict(cursor, row):
    names = [desc[0] for desc in cursor.description]
    return dict(zip(names, row))

def execute_query_to_dicts(connection, query, raise_on_error=False, run_id=None):
    if connection is None:
        message = "Cannot execute query because connection is not available."
        print(f"[Trino] {message}")
        if raise_on_error:
            raise RuntimeError(message)
        return []

    cursor = None
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        _register_query_id(run_id, cursor)
        return [row_to_dict(cursor, row) for row in cursor.fetchall()]
    except Exception as e:
        print(f"[Trino] Failed to execute query: {e}")
        if raise_on_error:
            raise
        return []
    finally:
        if cursor:
            cursor.close()
