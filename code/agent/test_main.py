import os

from code.agent.services.trino_service import connect_to_trino, execute_query, execute_query_to_dicts
from nodes.load_metadata_node import load_metadata

host = os.getenv("TRINO_HOST", "localhost")
port = int(os.getenv("TRINO_PORT", "8082"))
user = os.getenv("TRINO_USER", "agent4da")

connection = connect_to_trino(
    host=host,
    port=port,
    user=user,
    catalog="iceberg",
    schema="metadata"
)


metadata = load_metadata(connection)
print(metadata['tables'][:2])
print(metadata['columns'][:2])
print(metadata['metrics'][:2])
print(metadata['joins'][:2])