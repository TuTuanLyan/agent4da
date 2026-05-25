from helper.filter_metadata_rule import filter_metadata_by_question

def filter_schema_node(state):
    metadata = state["full_metadata"]
    question = state["user_question"]

    return {
        "filtered_metadata": filter_metadata_by_question(metadata, question)
    }


def build_schema_context(metadata):

    tables = metadata["tables"]
    columns = metadata["columns"]

    lines = []

    for table in tables:

        table_name = table["table_name"]

        lines.append(f"Table: {table_name}")
        lines.append(f"Purpose: {table['description']}")
        lines.append(f"Grain: {table['grain']}")

        table_columns = [
            c["column_name"]
            for c in columns
            if c["table_name"] == table_name
        ]

        lines.append(
            "Columns: " + ", ".join(table_columns[:15])
        )

        lines.append("")

    return "\n".join(lines)
