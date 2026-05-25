def table_matches_question(table_name, question):
    if table_name is None or question is None:
        return False
    
    question = question.lower()
    full_name = table_name.lower()
    short_name = full_name.split(".")[-1]

    return full_name in question or short_name in question


def filter_metadata_by_question(metadata, question):
    matched_tables = [
        table
        for table in metadata["tables"]
        if table_matches_question(table["table_name"], question)
    ]

    if not matched_tables:
        return metadata

    matched_table_names = {table["table_name"] for table in matched_tables}

    return {
        "tables": matched_tables,
        "columns": [
            column
            for column in metadata["columns"]
            if column["table_name"] in matched_table_names
        ],
        "metrics": [
            metric
            for metric in metadata["metrics"]
            if metric["base_table"] in matched_table_names
        ],
        "joins": [
            join
            for join in metadata["joins"]
            if (
                join["left_table"] in matched_table_names
                or join["right_table"] in matched_table_names
            )
        ],
    }
