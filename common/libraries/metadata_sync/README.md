# Metadata Sync Applicator

A PySpark utility library designed to automate and enforce data catalog governance in Databricks. This tool dynamically applies table-level and column-level comments (including descriptions and units) to existing database schemas based on a centrally managed dictionary. 

It includes robust preflight validation, schema drift detection, dry-run capabilities, and structured audit logging.

## Features
* **Automated Cataloging:** Applies `COMMENT ON` SQL statements to Databricks/Spark tables and columns programmatically.
* **Schema Drift Detection:** Automatically diffs your definitions against the actual database schema, warning you of undocumented columns or definitions for columns that no longer exist.
* **Safe Execution:** Includes a `dry_run` mode to preview changes without executing DDL statements.
* **Execution Auditing:** Returns a structured `RunSummary` that can be converted directly into a Spark DataFrame for audit logging.
* **Defensive Design:** Preflight checks ensure dictionary shapes are valid and target schemas exist before execution begins.

---

## 1. Expected Dictionary Structure

The core of the applicator is the `column_definitions` dictionary. It must follow a strict shape containing table comments and nested column descriptions.

```python
column_definitions = {
    "my_table_name": {
        "table": {
            "comment": "This table stores daily transaction aggregations."
        },
        "columns": {
            "transaction_id": {
                "description": {
                    "long_name": "Transaction Identifier",
                    "comment": "The unique UUID for the transaction.",
                    "unit": "not defined" # Optional: defaults to 'not defined'
                }
            },
            "amount_usd": {
                "description": {
                    "long_name": "Amount (USD)",
                    "comment": "The total transaction value in US Dollars.",
                    "unit": "USD"
                }
            }
        }
    }
}
```

*Note: The keys in the root dictionary should be the table names only (excluding catalog and database prefixes).*

---

## 2. Usage

Import the main applier function and pass in your Spark session, target schema, and definitions.

```python
from funcs.metadata_sync import apply_comments_for_all_tables

# 1. Run in Dry-Run mode to check for schema drift and validate definitions
summary = apply_comments_for_all_tables(
    spark=spark,
    catalog="my_catalog",
    database="my_database",
    column_definitions=column_definitions,
    dry_run=True, 
    fail_on_missing_table=False
)

# 2. View the execution summary as a DataFrame
display(summary.to_spark_df(spark))

# 3. Apply changes to the database
if not summary.errors:
    apply_comments_for_all_tables(
        spark=spark,
        catalog="my_catalog",
        database="my_database",
        column_definitions=column_definitions,
        dry_run=False 
    )
```

---

## 3. Configuration Parameters

The `apply_comments_for_all_tables` function accepts several parameters to control its behavior:

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `spark` | `SparkSession` | **Required** | The active Spark session. |
| `catalog` | `str` | **Required** | The target Unity Catalog name. |
| `database` | `str` | **Required** | The target database/schema name. |
| `column_definitions` | `Dict` | **Required** | The dictionary containing table and column metadata. |
| `comment_field` | `str` | `"comment"` | The key name to look for inside the definitions to extract the comment string. |
| `dry_run` | `bool` | `True` | If `True`, logs the SQL statements and drift warnings without executing DDL. |
| `fail_fast` | `bool` | `False` | If `True`, stops execution on the very first table error instead of proceeding. |
| `fail_on_any_error` | `bool` | `True` | If `True`, raises a `TableCommentsError` at the end of the run if any errors occurred. |
| `fail_on_missing_table`| `bool` | `False` | If `True`, treats a missing table as a hard error. If `False`, marks it as `skipped`. |
| `preflight_require_tables_exist` | `bool` | `False` | If `True`, the preflight check ensures at least one table in the definitions exists in the target database before starting. |

---

## 4. Audit Logging (`RunSummary`)

The function returns a `RunSummary` object containing a list of `TableRunResult` instances. This summary categorizes the run into `.ok`, `.skipped`, and `.errors`.

You can easily convert this summary into a Spark DataFrame to save to an audit log table:

```python
audit_df = summary.to_spark_df(spark)
audit_df.write.mode("append").saveAsTable("system_logs.metadata_audit_log")
```

The resulting DataFrame contains the following schema:
* `table_key`: The short name of the table.
* `full_table_name`: The fully qualified name (`catalog.database.table`).
* `status`: `ok`, `skipped`, `error`, or `ok_with_warnings`.
* `message`: Details regarding failures or skip reasons.
* `undefined_columns`: Array of strings (columns present in the database but missing from your definitions).
* `defined_but_missing`: Array of strings (columns in your definitions that don't exist in the database).