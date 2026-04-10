# This file is the "Rule Factory." It transforms the high-level YAML configurations into the specific JSON/SQL structures that the DQEngine understands.
import re
from pyspark.sql import functions as F

# helpers 
def resolve_role(table_cfg, role_name):
    '''
    Safely retrieves a role's columns from the table config.
    '''
    roles = table_cfg.get("roles", {})
    return roles.get(role_name)


def get_merge_cols(table_cfg):
    '''
    Ensures we always have a list of keys for the DQ engine to link results.
    '''
    roles = table_cfg.get("roles", {})
    res = roles.get("primary_keys") or roles.get("system_keys") or []
    return res if isinstance(res, list) else [res]


def get_fqtn(table_cfg):
    '''
    Retrieves the injected Fully Qualified Table Name.
    '''
    fqtn = table_cfg.get("fqtn")
    if not fqtn:
        table_name = table_cfg.get("table_name")
        raise ValueError(f"FQTN missing for {table_name}. Ensure 'source_db' is set in the runner.")
    return fqtn


def _get_scd2_context(params, table_cfg):
    '''
    Standardised context for SCD2 chronology checks.
    '''
    start_role = params.get("start_col_from_role", "system_start_at")
    end_role = params.get("end_col_from_role", "system_end_at")
    bk_val = resolve_role(table_cfg, "system_keys")

    if not bk_val:
        raise ValueError(f"SCD2 checks require 'system_keys' role for {table_cfg.get('table_name')}")

    start_col = resolve_role(table_cfg, start_role)
    end_col = resolve_role(table_cfg, end_role)

    if not start_col or not end_col:
        raise ValueError(
            f"SCD2 columns missing for {table_cfg.get('table_name')}. "
            f"Check roles '{start_role}' and '{end_role}'."
        )
    
    return {
        "bk": ", ".join(bk_val) if isinstance(bk_val, list) else bk_val,
        "start": resolve_role(table_cfg, start_role),
        "end": resolve_role(table_cfg, end_role),
        "merge": get_merge_cols(table_cfg),
        "fqtn": get_fqtn(table_cfg)
    }


# standard checks
def is_not_null(params, table_cfg):
    # Collect de-deuped columns from explicit list or multiple roles
    columns_to_check = set()
    
    # Get from explicit 'columns' param
    if params.get("columns"):
        cols = params.get("columns")
        columns_to_check.update(cols if isinstance(cols, list) else [cols])
    
    # Get from specific roles (e.g. [primary_keys, system_keys, business_keys])
    roles_to_pull = params.get("columns_from_roles") or [params.get("columns_from_role")]
    for role in roles_to_pull:
        if role:
            cols = resolve_role(table_cfg, role)
            if cols:
                columns_to_check.update(cols if isinstance(cols, list) else [cols])

    if not columns_to_check:
        raise ValueError(f"is_not_null check on {table_cfg.get('table_name')} has no columns to check.")

    # Generate one check per UNIQUE column
    return [{
        "name": f"{params.get('name', 'not_null')}_{c}",
        "criticality": params.get("criticality", "error"),
        "check": {"function": "is_not_null", "arguments": {"column": c}}
    } for c in sorted(list(columns_to_check))] # keep order consistent



def pk_unique(params, table_cfg):
    cols = params.get("columns") or resolve_role(table_cfg, params.get("columns_from_role"))
    if not cols:
        raise ValueError(f"pk_unique check on {table_cfg.get('table_name')} missing target columns.")
    return [{
        "name": params.get("name", "pk_unique"),
        "criticality": params.get("criticality", "error"),
        "check": {
            "function": "is_unique", 
            "arguments": {"columns": cols if isinstance(cols, list) else [cols], "nulls_distinct": params.get("nulls_distinct", False)}
        }
    }]


def is_in_range(params, table_cfg):
    columns_to_check = set()
    
    if params.get("columns"):
        cols = params.get("columns")
        columns_to_check.update(cols if isinstance(cols, list) else [cols])
    
    roles_to_pull = params.get("columns_from_roles") or [params.get("columns_from_role")]
    for role in roles_to_pull:
        if role:
            cols = resolve_role(table_cfg, role)
            if cols:
                columns_to_check.update(cols if isinstance(cols, list) else [cols])

    if not columns_to_check:
        raise ValueError(f"is_in_range check on {table_cfg.get('table_name')} missing target columns.")

    min_v = params.get("min_limit")
    max_v = params.get("max_limit")
    if min_v is None and max_v is None:
        raise ValueError(f"Range check requires at least min_limit or max_limit.")

    checks = []
    base_name = params.get("name", "range_check")
    
    for c in sorted(list(columns_to_check)):
        # If multiple columns are provided, append the column name to keep rule names unique
        rule_name = base_name if len(columns_to_check) == 1 else f"{base_name}_{c}"
        
        checks.append({
            "name": rule_name,
            "criticality": params.get("criticality", "warn"),
            "check": {
                "function": "is_in_range",
                "arguments": {"column": c, "min_limit": min_v, "max_limit": max_v},
            },
        })
        
    return checks


def no_future_timestamps(params, table_cfg):
    columns_to_check = set()
    
    if params.get("columns"):
        cols = params.get("columns")
        columns_to_check.update(cols if isinstance(cols, list) else [cols])
    
    roles_to_pull = params.get("columns_from_roles") or [params.get("columns_from_role")]
    for role in roles_to_pull:
        if role:
            cols = resolve_role(table_cfg, role)
            if cols:
                columns_to_check.update(cols if isinstance(cols, list) else [cols])

    fqtn = get_fqtn(table_cfg)
    if not columns_to_check:
        raise ValueError(f"no_future_timestamps on {table_cfg.get('table_name')} missing target columns.")

    checks = []
    base_name = params.get("name", "no_future_timestamps")
    
    for c in sorted(list(columns_to_check)):
        # If multiple columns are provided, append the column name to keep rule names unique
        rule_name = base_name if len(columns_to_check) == 1 else f"{base_name}_{c}"
        
        query = f"SELECT *, ({c} IS NOT NULL AND {c} > current_timestamp()) AS condition FROM {fqtn}"
        
        checks.append({
            "name": rule_name,
            "criticality": params.get("criticality", "error"),
            "check": {
                "function": "sql_query",
                "arguments": {
                    "query": query, 
                    "merge_columns": get_merge_cols(table_cfg), 
                    "condition_column": "condition"
                }
            }
        })
        
    return checks


def string_pattern_match(params, table_cfg):
    """
    Validates column(s) against a regex pattern.
    Commonly used for IDs like 'LIFE_MILE_1234' or 'ASSET_INFRA_5678'.
    """
    columns_to_check = set()
    
    # Check for the plural 'columns'
    if params.get("columns"):
        cols = params.get("columns")
        columns_to_check.update(cols if isinstance(cols, list) else [cols])
    
    # Handle role resolution
    roles_to_pull = params.get("columns_from_roles") or [params.get("columns_from_role")]
    for role in roles_to_pull:
        if role:
            cols = resolve_role(table_cfg, role)
            if cols:
                columns_to_check.update(cols if isinstance(cols, list) else [cols])

    pattern = params.get("pattern")
    fqtn = get_fqtn(table_cfg)
    
    if not columns_to_check or not pattern:
        raise ValueError(f"string_pattern_match on {table_cfg.get('table_name')} requires 'columns' and 'pattern'.")

    # We must escape backslashes again so PySpark SQL interprets the string literal correctly
    safe_pattern = pattern.replace('\\', '\\\\')
    base_name = params.get("name", "pattern_match")
    checks = []

    for col in sorted(list(columns_to_check)):
        rule_name = base_name if len(columns_to_check) == 1 else f"{base_name}_{col}"
        
        # Explicit FAILURE condition: It fails if it is NOT NULL and DOES NOT match the regex.
        query = f"SELECT *, ({col} IS NOT NULL AND {col} NOT RLIKE '{safe_pattern}') AS condition FROM {fqtn}"
        friendly_msg = f"Column '{col}' contains a value that does not match the required pattern: {pattern}"

        checks.append({
            "name": rule_name,
            "criticality": params.get("criticality", "warn"),
            "check": {
                "function": "sql_query",
                "arguments": {
                    "query": query,
                    "merge_columns": get_merge_cols(table_cfg),
                    "condition_column": "condition",
                    "msg": friendly_msg
                }
            }
        })
        
    return checks


def fk_exists(params, table_cfg):
    name = params.get("name", "fk_integrity_check")
    criticality = params.get("criticality", "error")
    allow_nulls = params.get("allow_nulls", False)
    
    fk_cols = params.get("fk_columns")   # The columns in the CURRENT table
    ref_cols = params.get("ref_columns") # The target aliases (what we want to call them)
    fqtn = get_fqtn(table_cfg)
    
    # Normalise ref_table
    raw_ref = params.get("ref_table")
    ref_tables = raw_ref if isinstance(raw_ref, list) else [raw_ref] if raw_ref else []

    # Normalise source_ref_cols (the actual names in the dim tables)
    # If not provided, assume they match ref_cols
    source_ref_cols = params.get("source_ref_columns")
    if not source_ref_cols:
        source_ref_cols = [ref_cols] * len(ref_tables)
    elif not isinstance(source_ref_cols[0], list):
        source_ref_cols = [source_ref_cols]

    if not all([fk_cols, ref_tables, ref_cols, fqtn]):
        raise ValueError(f"fk_exists on {table_cfg.get('table_name')} missing required params.")

    # Resolve table paths
    prefix = ".".join(fqtn.split(".")[:2])
    resolved_refs = [t if "." in t else f"{prefix}.{t}" for t in ref_tables]

    # Build Union CTE with Aliasing
    # E.g. SELECT asset_infra_id as asset_id FROM table_a UNION SELECT asset_plat_id as asset_id FROM table_b
    union_parts = []
    for i, table_path in enumerate(resolved_refs):
        select_clause = ", ".join([f"{src} AS {target}" for src, target in zip(source_ref_cols[i], ref_cols)])
        union_parts.append(f"SELECT {select_clause} FROM {table_path}")
    
    union_query = " UNION ".join(union_parts)

    # Join logic (remains the same)
    merge_columns = get_merge_cols(table_cfg)
    fk_join = " AND ".join([f"i.{fk} = r.{rc}" for fk, rc in zip(fk_cols, ref_cols)])
    
    if allow_nulls:
        # Only fail if it's NOT NULL but missing in dim
        pop_check = " OR ".join([f"i.{c} IS NOT NULL" for c in fk_cols])
        failure_condition = f"({pop_check}) AND r.{ref_cols[0]} IS NULL"
    else:
        # Fail if it's NULL OR if it's missing in Dim
        failure_condition = f"r.{ref_cols[0]} IS NULL"
    
    query = f"""
    WITH r AS (
        SELECT DISTINCT {", ".join(ref_cols)} FROM (
            {union_query}
        )
    ),
    x AS (
      SELECT
          i.*,
          ( {failure_condition} ) AS condition
      FROM {fqtn} i
      LEFT JOIN r ON {fk_join}
    )
    SELECT *, condition FROM x
    """
    return [{
        "name": name,
        "criticality": criticality,
        "check": {
            "function": "sql_query",
            "arguments": {
                "query": query,
                "merge_columns": merge_columns,
                "condition_column": "condition",
                "input_placeholder": "i",
                "msg": f"FK mismatch on columns {fk_cols}. Missing in: {', '.join(ref_tables)} (allow_nulls={allow_nulls})",
            },
        },
    }]


# --- SCD2 Checks ---
def scd2_pk_unique(params, table_cfg):
    """
    Validates that the combination of Keys + Start Date is unique.
    Used for SCD2 tables where a PK repeats but with different start dates.
    """
    # Determine if we are checking system_keys or business_keys based on the name or params
    # We'll check for 'system_keys' first as a default for SCD2
    key_role = "business_keys" if "business" in params.get("name", "") else "system_keys"
    
    keys = resolve_role(table_cfg, key_role)
    start_col = resolve_role(table_cfg, params.get("start_col_from_role"))

    if not keys or not start_col:
        raise ValueError(f"scd2_pk_unique on {table_cfg.get('table_name')} missing keys or start_col.")

    # Combine them: e.g. [asset_id, system_start_at]
    combined_cols = (keys if isinstance(keys, list) else [keys]) + [start_col]

    return [{
        "name": params.get("name", "scd2_pk_unique"),
        "criticality": params.get("criticality", "error"),
        "check": {
            "function": "is_unique", 
            "arguments": {"columns": combined_cols, "nulls_distinct": False}
        }
    }]


def scd2_no_overlap(params, table_cfg):
    ctx = _get_scd2_context(params, table_cfg)
    query = f"""
    SELECT *, 
           (LAG({ctx['end']}) OVER (PARTITION BY {ctx['bk']} ORDER BY {ctx['start']}) > {ctx['start']}) AS condition
    FROM {ctx['fqtn']}
    """
    return [{
        "name": params.get("name", "scd2_overlap"),
        "criticality": params.get("criticality", "error"),
        "check": {
            "function": "sql_query",
            "arguments": {"query": query, "merge_columns": ctx['merge'], "condition_column": "condition"}
        }
    }]


def scd2_contiguous(params, table_cfg):
    ctx = _get_scd2_context(params, table_cfg)
    query = f"""
    SELECT *, (next_start IS NOT NULL AND {ctx['end']} != next_start) AS condition
    FROM (
      SELECT *, LEAD({ctx['start']}) OVER (PARTITION BY {ctx['bk']} ORDER BY {ctx['start']}) AS next_start
      FROM {ctx['fqtn']}
    )
    """
    return [{
        "name": params.get("name", "scd2_not_contiguous"),
        "criticality": params.get("criticality", "error"),
        "check": {
            "function": "sql_query",
            "arguments": {"query": query, "merge_columns": ctx['merge'], "condition_column": "condition"}
        }
    }]


def scd2_start_after_end(params, table_cfg):
    ctx = _get_scd2_context(params, table_cfg)
    query = f"SELECT *, ({ctx['start']} IS NOT NULL AND {ctx['end']} IS NOT NULL AND {ctx['start']} > {ctx['end']}) AS condition FROM {ctx['fqtn']}"
    return [{
        "name": params.get("name", "scd2_chronology_error"),
        "criticality": params.get("criticality", "error"),
        "check": {
            "function": "sql_query",
            "arguments": {"query": query, "merge_columns": ctx['merge'], "condition_column": "condition"}
        }
    }]


def scd2_zero_length_interval(params, table_cfg):
    ctx = _get_scd2_context(params, table_cfg)
    query = f"SELECT *, ({ctx['start']} IS NOT NULL AND {ctx['end']} IS NOT NULL AND {ctx['start']} = {ctx['end']}) AS condition FROM {ctx['fqtn']}"
    return [{
        "name": params.get("name", "scd2_zero_length_interval"),
        "criticality": params.get("criticality", "error"),
        "check": {
            "function": "sql_query",
            "arguments": {"query": query, "merge_columns": ctx['merge'], "condition_column": "condition"}
        }
    }]


# check registry
GLOBAL_REGISTRY = {
    "is_not_null": is_not_null,
    "pk_unique": pk_unique,
    "no_future_timestamps": no_future_timestamps,
    "is_in_range": is_in_range,
    "string_pattern_match": string_pattern_match,
    "fk_exists": fk_exists,
    "scd2_no_overlap": scd2_no_overlap,
    "scd2_contiguous": scd2_contiguous,
    "scd2_start_after_end": scd2_start_after_end,
    "scd2_zero_length_interval": scd2_zero_length_interval,
    "scd2_pk_unique": scd2_pk_unique
}