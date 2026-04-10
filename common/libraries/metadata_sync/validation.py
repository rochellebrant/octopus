from __future__ import annotations

from typing import Any, Dict, Set, Tuple

from .models import TableCommentsError


def warn(msg: str) -> None:
    print(f">>> ⚠️ WARNING: {msg}")


def info(msg: str) -> None:
    print(f">>> INFO: {msg}")


def sql_escape(s: str) -> str:
    return s.replace("'", "''")


def validate_column_definitions_shape(column_definitions: Dict[str, Any]) -> None:
    if not isinstance(column_definitions, dict) or not column_definitions:
        raise ValueError("column_definitions must be a non-empty dict")

    for table_key, table_def in column_definitions.items():
        if not isinstance(table_key, str) or not table_key.strip():
            raise ValueError(f"Invalid table_key in column_definitions: {table_key!r}")
        if not isinstance(table_def, dict):
            raise TypeError(f"Definition for {table_key} must be a dict")

        cols = (table_def.get("columns", {}) or {})
        if not isinstance(cols, dict):
            raise TypeError(f"column_definitions['{table_key}']['columns'] must be a dict")

        for col_name, col_def in cols.items():
            if not isinstance(col_def, dict):
                raise TypeError(f"column_definitions['{table_key}']['columns']['{col_name}'] must be a dict")
            desc = (col_def.get("description", {}) or {})
            if not isinstance(desc, dict):
                raise TypeError(
                    f"column_definitions['{table_key}']['columns']['{col_name}']['description'] must be a dict"
                )


def list_tables_in_schema(spark, catalog: str, database: str) -> Set[str]:
    rows = spark.sql(f"SHOW TABLES IN {catalog}.{database}").select("tableName").collect()
    return {r["tableName"] for r in rows}


def preflight(
    spark,
    catalog: str,
    database: str,
    column_definitions: Dict[str, Any],
    require_tables_exist: bool = False,
) -> Set[str]:
    if not catalog or not database:
        raise ValueError("catalog and database must be provided")

    validate_column_definitions_shape(column_definitions)

    try:
        existing_tables = list_tables_in_schema(spark, catalog, database)
    except Exception as e:
        raise TableCommentsError(f"Preflight failed: cannot access schema {catalog}.{database}: {e}")

    if require_tables_exist:
        intersection = set(column_definitions.keys()) & existing_tables
        if not intersection:
            defined_tables = sorted(list(column_definitions.keys()))
            found_tables = sorted(list(existing_tables))
            
            # Print exactly what we are comparing so you can visually spot the difference
            info(f"DEBUG - Tables defined in dictionary: {defined_tables}")
            info(f"DEBUG - Tables found in {catalog}.{database}: {found_tables}")
            
            raise TableCommentsError(
                f"Preflight failed: none of the tables in column_definitions exist in {catalog}.{database}.\n"
                f"--> Defined keys: {defined_tables}\n"
                f"--> Found tables: {found_tables}"
            )

    return existing_tables


def get_table_columns(spark, full_table_name: str) -> Set[str]:
    rows = spark.sql(f"SHOW COLUMNS IN {full_table_name}").collect()
    if not rows:
        return set()

    first = rows[0].asDict()
    if "col_name" in first:
        key = "col_name"
    elif "column_name" in first:
        key = "column_name"
    else:
        key = list(first.keys())[0]

    cols: Set[str] = set()
    for r in rows:
        v = r.asDict().get(key)
        if v:
            cols.add(v)

    return cols


def diff_columns(defined: Set[str], actual: Set[str]) -> Tuple[Set[str], Set[str]]:
    """
    Returns (undefined_in_def, defined_but_missing)
    - undefined_in_def: columns present in table but missing from definitions
    - defined_but_missing: columns defined but missing from table
    """
    return (actual - defined, defined - actual)
