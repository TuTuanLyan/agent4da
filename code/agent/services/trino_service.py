from trino.dbapi import connect

def connect_to_trino(host, port, user, catalog, schema):
    try:
        connection = connect(
            host=host,
            port=port,
            user=user,
            catalog=catalog,
            schema=schema
        )
        print("[Trino] Connection to Trino established successfully.")
        return connection
    except Exception as e:
        print(f"[Trino] Failed to connect to Trino: {e}")
        return None

def execute_query(connection, query):
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        return results
    except Exception as e:
        print(f"[Trino] Failed to execute query: {e}")
        return None
    finally:
        cursor.close()
    
def row_to_dict(cursor, row):
    names = [desc[0] for desc in cursor.description]
    return dict(zip(names, row))

def execute_query_to_dicts(connection, query):
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        return [row_to_dict(cursor, row) for row in cursor.fetchall()]
    except Exception as e:
        print(f"[Trino] Failed to execute query: {e}")
        return None
    finally:
        cursor.close()