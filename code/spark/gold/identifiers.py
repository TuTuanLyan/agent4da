"""Identifier and location guards for Gold Iceberg tasks."""

import re


IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_identifier_part(value, field_name):
    if value is None or str(value).strip() == "":
        raise ValueError(f"{field_name} must not be empty.")

    value = str(value).strip()
    if not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(
            f"Invalid {field_name}: {value!r}. Use letters, digits, and underscores; "
            "the first character must be a letter or underscore."
        )
    return value


def quote_identifier(identifier):
    parts = str(identifier).split(".")
    return ".".join(f"`{validate_identifier_part(part, 'identifier part')}`" for part in parts)


def table_identifier(catalog, namespace, table):
    catalog = validate_identifier_part(catalog, "catalog")
    namespace = validate_identifier_part(namespace, "namespace")
    table = validate_identifier_part(table, "table")
    return f"{catalog}.{namespace}.{table}"


def quote_sql_string(value):
    value = str(value)
    if "\x00" in value:
        raise ValueError("SQL string contains a null byte.")
    return "'" + value.replace("'", "''") + "'"


def assert_safe_table_location(path, allowed_prefixes):
    if path is None or str(path).strip() == "":
        raise ValueError("Table location must not be empty.")

    path = str(path).strip()
    if "'" in path or "\x00" in path:
        raise ValueError(f"Unsafe table location: {path!r}")

    if not any(path.startswith(prefix) for prefix in allowed_prefixes):
        allowed = ", ".join(allowed_prefixes)
        raise ValueError(
            f"Unsafe table location: {path!r}. Expected one of prefixes: {allowed}"
        )
