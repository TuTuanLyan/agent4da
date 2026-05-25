from nodes.load_metadata_node import load_metadata
from nodes.filter_schema_node import filter_metadata_by_question

question = "Cho tôi xem dữ liệu trong bảng fact_events"

metadata = load_metadata()
print(metadata['tables'][0])
print(metadata['columns'][0])
print(metadata['metrics'][0])
print(metadata['joins'][0])

filtered_metadata = filter_metadata_by_question(metadata, question)
print("Filtered tables:")
for table in filtered_metadata["tables"]:
    print(table["table_name"])
