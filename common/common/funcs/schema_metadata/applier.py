from __future__ import annotations 

from typing import Any, Dict

from .models import RunSummary, TableCommentsError, TableRunResult
from .validation import (
    diff_columns,
    get_table_columns,
    info,
    preflight,
    sql_escape,
    warn,
)


def _exec_sql(spark, sql_stmt: str, dry_run: bool) -> None:
    if dry_run:
        info(sql_stmt)
    else:
        spark.sql(sql_stmt)


def apply_comments_from_definitions(
    spark,
    full_table_name: str,
    table_key: str,
    column_definitions: Dict[str, Any],
    result_row: TableRunResult,
    comment_field: str = "comment",
    dry_run: bool = True,
) -> None:
    if table_key not in column_definitions:
        warn(f"Table not found in definitions: {table_key}")
        result_row.mark_warning()
        result_row.add_message("Table missing from definitions.")
        return

    table_def = column_definitions[table_key]

    # ----- table comment
    table_comment = (table_def.get("table", {}) or {}).get(comment_field)
    if table_comment:
        sql_stmt = f"COMMENT ON TABLE {full_table_name} IS '{sql_escape(str(table_comment).strip())}'"
        _exec_sql(spark, sql_stmt, dry_run)
        if not dry_run:
            info(f"Applied table comment on {full_table_name}")
    else:
        warn(f"No table comment found for {table_key} under ['table']['{comment_field}']")
        result_row.mark_warning()
        result_row.add_message("No table comment found in definitions.")

    # ----- column diff / warnings
    defined_cols = table_def.get("columns", {}) or {}
    defined_col_names = set(defined_cols.keys())
    actual_col_names = get_table_columns(spark, full_table_name)

    undefined_in_def, defined_but_missing = diff_columns(defined_col_names, actual_col_names)

    result_row.undefined_columns = sorted(undefined_in_def)
    result_row.defined_but_missing = sorted(defined_but_missing)

    if undefined_in_def:
        result_row.mark_warning()
        result_row.add_message(
            f"{len(undefined_in_def)} columns exist in the table but are missing from definitions (skipped)."
        )
        warn(
            f"{len(undefined_in_def)} columns exist in {full_table_name} but are missing from definitions. "
            f"Skipping: {sorted(undefined_in_def)}"
        )

    if defined_but_missing:
        result_row.mark_warning()
        result_row.add_message(
            f"{len(defined_but_missing)} columns are defined but do not exist in the table (skipped)."
        )
        warn(
            f"{len(defined_but_missing)} columns are defined but do not exist in {full_table_name}. "
            f"Skipping: {sorted(defined_but_missing)}"
        )

    # ----- apply comments for intersecting columns only
    for col in sorted(actual_col_names & defined_col_names):
        col_def = defined_cols[col]
        col_desc = col_def.get("description", {}) or {}

        col_comment = col_desc.get(comment_field)
        col_unit = col_desc.get("unit", "not defined")
        col_long_name = col_desc.get("long_name", "not defined")

        if col_comment:
            sql_stmt = (
                f"COMMENT ON COLUMN {full_table_name}.{col} IS "
                f"'{sql_escape(str(col_long_name).strip())}. {sql_escape(str(col_comment).strip())}"
                f" \nUNIT: {sql_escape(str(col_unit).strip())}'"
            )
            _exec_sql(spark, sql_stmt, dry_run)
            if not dry_run:
                info(f"Applied column comment on {full_table_name}.{col}")
        else:
            warn(
                f"No column comment found for {table_key}.{col} under "
                f"['columns']['{col}']['description']['{comment_field}']"
            )
            result_row.mark_warning()
            result_row.add_message(f"Missing comment for column {col}.")


def apply_comments_for_all_tables(
    spark,
    catalog: str,
    database: str,
    column_definitions: Dict[str, Any],
    comment_field: str = "comment",
    dry_run: bool = True,
    fail_fast: bool = False,
    fail_on_any_error: bool = True,
    fail_on_missing_table: bool = False,
    preflight_require_tables_exist: bool = False,
) -> RunSummary:
    existing_tables = preflight(
        spark=spark,
        catalog=catalog,
        database=database,
        column_definitions=column_definitions,
        require_tables_exist=preflight_require_tables_exist,
    )

    summary = RunSummary()

    if dry_run:
        info("DRY RUN ACTIVE - no changes will be made")

    for table_key in column_definitions.keys():
        full_table_name = f"{catalog}.{database}.{table_key}"

        # missing table check
        if table_key not in existing_tables:
            msg = f"Table does not exist: {full_table_name}"
            if fail_on_missing_table:
                summary.add(table_key, full_table_name, "error", msg)
                warn(msg)
                if fail_fast:
                    break
            else:
                summary.add(table_key, full_table_name, "skipped", msg)
                warn(msg + " — skipping")
            continue

        info(f"Processing table: {full_table_name}")
        row = summary.add(table_key, full_table_name, "ok")

        try:
            apply_comments_from_definitions(
                spark=spark,
                full_table_name=full_table_name,
                table_key=table_key,
                column_definitions=column_definitions,
                result_row=row,
                comment_field=comment_field,
                dry_run=dry_run,
            )
        except Exception as e:
            msg = str(e)
            row.status = "error"
            row.message = msg
            warn(f"Failed to apply comments for {full_table_name}: {msg}")
            if fail_fast:
                break

        print("-" * 90)

    info(f"Run summary: ok={len(summary.ok)} skipped={len(summary.skipped)} errors={len(summary.errors)}")

    if summary.errors:
        for r in summary.errors[:20]:
            warn(f"ERROR: {r.full_table_name} ({r.table_key}) -> {r.message}")
        if len(summary.errors) > 20:
            warn(f"...and {len(summary.errors) - 20} more errors")

    tables_with_col_warnings = [
        r for r in summary.ok
        if r.undefined_columns or r.defined_but_missing
    ]
    info(f"Tables with column mismatches: {len(tables_with_col_warnings)}")

    for r in tables_with_col_warnings[:20]:
        if r.undefined_columns:
            warn(f"{r.full_table_name}: undefined_columns={r.undefined_columns}")
        if r.defined_but_missing:
            warn(f"{r.full_table_name}: defined_but_missing={r.defined_but_missing}")

    if fail_on_any_error and summary.errors:
        raise TableCommentsError(
            f"Comment application failed with {len(summary.errors)} error(s). See log output above for details."
        )

    return summary
