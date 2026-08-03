# Databricks Data Quality (DQ) Framework

A highly scalable, metadata-driven Data Quality engine built on top of [Databricks Labs DQX](https://databrickslabs.github.io/dqx/). This framework decouples business validation logic from PySpark execution, allowing data stewards and engineers to define complex data quality rules via YAML configurations, AI prompts, or custom Python sidecars.

Results are automatically evaluated, split by criticality (`error` vs. `warn`), and exploded into structured Delta Lake audit tables for downstream observability.

## Features
* **Metadata-Driven Execution:** Define rules in easy-to-read YAML files. Merges table-specific rules with global/folder-level `defaults.yaml`.
* **GenAI-Assisted Rules:** Automatically generate complex validation logic at runtime using natural language AI prompts.
* **Extensible "Rule Factory":** Comes loaded with complex templates (including cross-table FK checks and SCD2 temporal validations).
* **Python Sidecars:** Inject bespoke, highly specific PySpark logic via `{table_name}.py` files alongside your YAML configs.
* **Granular Audit Logging:** Automatically logs row-level violations to a standard Delta table (`std__{table_name}`) containing the failed rule, message, column, and runtime metadata.

---

## 1. Directory Structure

The framework expects your configurations to be organized into groups. Each group can have a `defaults.yaml` (applied to all tables in the folder) and specific `{table_name}.yaml` files.

```text
configs/
└── my_domain/
    ├── defaults.yaml        # Global roles and checks
    ├── customer_dim.yaml    # Table-specific config
    └── customer_dim.py      # (Optional) Python sidecar for bespoke rules
```

## 2. Configuration (YAML)

Configurations use `roles` to group columns logically, which rules can reference dynamically. Rules belong to `checks` and can be defined using built-in templates, AI prompts, or custom sidecar functions.

**Example `customer_dim.yaml`:**
```yaml
table_name: "customer_dim"

roles:
  primary_keys: ["customer_id"]
  system_keys: ["customer_id"]
  system_start_at: "valid_from"
  system_end_at: "valid_to"

checks:
  # 1. Built-in Template Check
  - template: "is_not_null"
    criticality: "error"
    params:
      columns_from_role: "primary_keys"
  
  # 2. Complex Template (Foreign Key)
  - template: "fk_exists"
    criticality: "warn"
    params:
      fk_columns: ["region_id"]
      ref_table: "region_dim"
      ref_columns: ["region_id"]
  
  # 3. GenAI Prompt
  - ai_prompt: "Ensure that email_address contains a valid '@' symbol and a domain."
    criticality: "error"
    name: "valid_email_ai"

  # 4. Custom Sidecar Function (defined in customer_dim.py)
  - template: "bespoke_business_logic"
    criticality: "error"
    params:
      threshold: 100
```

## 3. Usage

To run the DQ framework for a specific table, initialize the `DQRunner` with your Spark session and environment runtime details.

```python
from dq_framework.engine import DQRunner

# 1. Define runtime target environments
runtime_config = {
    "source_db": "hive_metastore.bronze_layer",
    "dq_db": "hive_metastore.dq_audit_layer"
}

# 2. Initialize Runner
runner = DQRunner(
    spark=spark, 
    runtime=runtime_config, 
    base_path="/Workspace/Shared/dq_framework"
)

# 3. Execute Checks
results = runner.run_for_table(
    config_rel_path="my_domain", 
    table_name="customer_dim"
)

# 4. View Run Summary
print(f"Status: {results['emoji']} {results['status']}")
print(f"Success Rate: {results['success_rate']}%")
print(f"Failed Rows: {results['failed_rows']}")
```

## 4. Built-In Rule Templates (`GLOBAL_REGISTRY`)

The engine's Rule Factory comes pre-loaded with the following templates:

**Standard Checks:**
* `is_not_null`: Validates columns (or roles) contain no nulls.
* `pk_unique`: Ensures primary keys are entirely unique.
* `is_in_range`: Validates numeric columns fall between `min_limit` and `max_limit`.
* `no_future_timestamps`: Flags timestamps occurring in the future.
* `string_pattern_match`: Validates columns against a Regex `pattern`.
* `fk_exists`: Validates referential integrity against single or multiple target dimension tables.

**SCD2 Temporal Checks:**
* `scd2_pk_unique`: Validates uniqueness of Business Keys + Start Date.
* `scd2_no_overlap`: Ensures active periods for a given key do not overlap.
* `scd2_contiguous`: Ensures no gaps exist between sequential temporal intervals.
* `scd2_start_after_end`: Catches chronological inversions (start > end).
* `scd2_zero_length_interval`: Flags records where start and end times are exactly equal.

---

## 5. Audit Outputs

When the framework executes, it parses the results from the DQX engine, combines warnings and errors, and explodes the nested JSON output into a flattened Delta table.

**Output Table:** `{dq_db}.std__{table_name}`

**Schema:**
* `_check_name`: The name of the failed rule (e.g., `is_not_null_customer_id`).
* `_check_criticality`: `ERROR` or `WARN`.
* `_check_function`: The underlying Spark/DQX function used.
* `_check_message`: A human-readable failure description.
* `_check_column`: The specific column that violated the rule.
* `_check_run_time`: Execution timestamp.
* `_check_run_id`: Unique UUID for the run batch.
* `[...source_columns]`: All original columns from the source table for easy debugging.