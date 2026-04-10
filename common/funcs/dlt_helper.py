from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    ArrayType,
)
from datetime import datetime
from functools import reduce
from pyspark.sql import DataFrame, functions as f
from pyspark.sql.utils import AnalysisException

DQ_HARD_PREFIX = "DQ_hard_"
DQ_SOFT_PREFIX = "DQ_soft_"


def stream_reader(spark, table):
    """
    Reads a table as a streaming DataFrame, ignoring change commits.

    Args:
        spark (SparkSession): The active Spark session.
        table (str): The full path or name of the table to read.

    Returns:
        DataFrame: A streaming PySpark DataFrame.
    """
    df = spark.readStream.option("skipChangeCommits", "true").table(table)
    return df


def batch_reader(spark, table):
    """
    Reads a table as a static batch DataFrame.

    Args:
        spark (SparkSession): The active Spark session.
        table (str): The full path or name of the table to read.

    Returns:
        DataFrame: A static PySpark DataFrame.
    """
    df = spark.read.table(table)
    return df


def cast_columns(
    df: DataFrame, column_definitions: dict, table_config: dict, table: str
) -> DataFrame:
    """
    Casts DataFrame columns to specific data types based on user definitions.
    Safely strips JSON quotes from Variant extractions and handles ISO formatting natively.
    """
    if not column_definitions or table not in column_definitions:
        print(f"WARNING: Table '{table}' missing or no definitions provided. Skipping casting.")
        return df
        
    expected_columns = column_definitions[table].get("columns", {})
    existing_cols_map = {c.lower(): c for c in df.columns}
    
    for defined_col in expected_columns.keys():
        if defined_col.lower() not in existing_cols_map:
            print(f"WARNING: Column '{defined_col}' defined for casting but not found in data for table '{table}'.")
            
    expected_schema_cleaned = {}
    df_col_to_user_col = {}

    for user_col_name, col_data in expected_columns.items():
        if user_col_name.lower() in existing_cols_map:
            actual_df_name = existing_cols_map[user_col_name.lower()]
            data_type = col_data.get("schema", {}).get("data_type", "STRING").strip().upper()
            
            expected_schema_cleaned[actual_df_name] = data_type
            df_col_to_user_col[actual_df_name] = user_col_name

    table_params = table_config.get(table, {})
    timestamp_formats = {existing_cols_map.get(k.lower(), k): v for k, v in table_params.get("timestamp_formats", {}).items()}
    date_formats = {existing_cols_map.get(k.lower(), k): v for k, v in table_params.get("date_formats", {}).items()}
    
    # --- VALIDATION CHECKS RESTORED HERE ---
    for col, fmt in timestamp_formats.items():
        if col not in existing_cols_map.values():
            continue
        if expected_schema_cleaned.get(col, "") != "TIMESTAMP":
            raise ValueError(f"Column '{col}' in table '{table}' has a timestamp format but is defined as {expected_schema_cleaned.get(col, 'UNKNOWN')}.")

    for col, fmt in date_formats.items():
        if col not in existing_cols_map.values():
            continue
        if expected_schema_cleaned.get(col, "") != "DATE":
            raise ValueError(f"Column '{col}' in table '{table}' has a date format but is defined as {expected_schema_cleaned.get(col, 'UNKNOWN')}.")
    # ---------------------------------------

    select_exprs = []
    for field in df.schema:
        actual_name = field.name
        if actual_name in expected_schema_cleaned:
            target_type = expected_schema_cleaned[actual_name]
            user_case_name = df_col_to_user_col[actual_name]
            
            # 1. Strip literal JSON quotes (^" at start, "$ at end) from Variant extraction
            clean_col = f.regexp_replace(f.col(actual_name), '^"|"$', '')

            # 2. Smarter Casting Logic
            if target_type == "TIMESTAMP":
                if actual_name in timestamp_formats:
                    fmt = timestamp_formats[actual_name]
                    select_exprs.append(f.to_timestamp(clean_col, fmt).alias(user_case_name))
                else:
                    # Native cast naturally handles ISO 8601 strings (the 'T') without failing!
                    select_exprs.append(clean_col.cast("timestamp").alias(user_case_name))
                    
            elif target_type == "DATE":
                if actual_name in date_formats:
                    fmt = date_formats[actual_name]
                    select_exprs.append(f.to_date(f.to_timestamp(clean_col, fmt)).alias(user_case_name))
                else:
                    select_exprs.append(f.to_date(clean_col.cast("timestamp")).alias(user_case_name))
                    
            else:
                select_exprs.append(clean_col.cast(target_type).alias(user_case_name))
        else:
            select_exprs.append(f.col(actual_name))
            
    return df.select(*select_exprs)


def apply_column_comments(df: DataFrame, table: str, column_definitions: dict) -> DataFrame:
    """
    Safely injects column comments directly into the DataFrame's metadata.
    Handles case-mismatches between user definitions and actual DF columns.

    Args:
        df (DataFrame): The input PySpark DataFrame.
        table (str): The name of the table being processed.
        column_definitions (dict): Dictionary mapping columns to their target descriptions.

    Returns:
        DataFrame: The DataFrame with comments appended to column metadata.
    """
    if not column_definitions or table not in column_definitions:
        return df
        
    table_def = column_definitions[table].get("columns", {})
    lookup = {k.lower(): v for k, v in table_def.items()}
    
    for col_name in df.columns:
        match = lookup.get(col_name.lower())
        if match:
            comment = match.get("description", {}).get("comment", "")
            if comment:
                df = df.withMetadata(col_name, {"comment": comment})
                
    return df


def build_static_schema(table: str, column_definitions: dict) -> str:
    """
    Constructs a DDL string purely from the provided static definitions dictionary.
    Bypasses Spark DataFrame reads entirely, preventing Delta Live Tables (DLT) DAG 
    compilation errors that occur when referencing internal 'live.' datasets before 
    they are materialized during the planning phase.

    Args:
        table (str): The name of the table being processed.
        column_definitions (dict): Dictionary containing target data types and comments.

    Returns:
        str: A fully formatted DDL string for DLT schema declaration, or None if definitions are missing or empty.
    """
    if not column_definitions or table not in column_definitions:
        return None

    table_def = column_definitions[table].get("columns", {})
    if not table_def:
        return None

    schema_parts = []
    
    for col_name, col_data in table_def.items():
        data_type = col_data.get("schema", {}).get("data_type", "STRING").strip().upper()
        comment = col_data.get("description", {}).get("comment", "")
        comment = str(comment).replace("'", "\\'") if comment else ""
        
        if comment:
            schema_parts.append(f"`{col_name}` {data_type} COMMENT '{comment}'")
        else:
            schema_parts.append(f"`{col_name}` {data_type}")

    return ", ".join(schema_parts)
    

def build_live_schema(spark, input_table_full_path, table_key, column_definitions):
    """
    Constructs a DDL string from an existing source table's schema, merging in data types 
    and comments from the provided definitions. Applies the exact column casing specified 
    by the user in the definitions to ensure alignment with downstream casting.
    It also appends any extra columns defined in the dictionary that are not present 
    in the raw source (e.g., ingestion timestamps).

    Args:
        spark (SparkSession): The active Spark session.
        input_table_full_path (str): The physical path of the external table to use as a baseline.
        table_key (str): The name of the table being processed.
        column_definitions (dict): Dictionary containing target data types and comments.

    Returns:
        str: A fully formatted DDL string for DLT schema declaration, or None if reading fails.
    """
    try:
        source_df = spark.read.table(input_table_full_path)
        actual_schema = source_df.schema
    except AnalysisException as e:
        print(f"WARNING: Could not read schema from '{input_table_full_path}'. "
              f"Falling back to dynamic schema inference (comments will be lost). "
              f"Underlying error: {str(e)}")
        return None
    except Exception as e:
        print(f"ERROR: Unexpected failure while reading schema from '{input_table_full_path}'.")
        raise e

    if not column_definitions or table_key not in column_definitions:
        return actual_schema.toDDL()

    table_def = column_definitions[table_key].get("columns", {})
    
    # Store both the exact user casing and the metadata in our lookup
    lookup = {k.lower(): {"user_name": k, "meta": v} for k, v in table_def.items()}
    
    schema_parts = []
    added_cols = set()
    
    # 1. Map columns that exist in the raw table
    for field in actual_schema:
        col_name = field.name
        col_type = field.dataType.simpleString().upper()
        
        match_data = lookup.get(col_name.lower())
        if match_data:
            # Override the source table's casing with the user's defined casing!
            final_col_name = match_data["user_name"]
            meta = match_data["meta"]
            
            defined_type = meta.get("schema", {}).get("data_type")
            if defined_type:
                col_type = defined_type.strip().upper()

            comment = meta.get("description", {}).get("comment", "")
            comment = comment.replace("'", "\\'")
        else:
            # If not in definitions, just use the raw source casing
            final_col_name = col_name
            comment = ""
        
        if comment:
            schema_parts.append(f"`{final_col_name}` {col_type} COMMENT '{comment}'")
        else:
            schema_parts.append(f"`{final_col_name}` {col_type}")
            
        # Track by lowercase to ensure we don't accidentally duplicate columns later
        added_cols.add(final_col_name.lower())

    # 2. Append extra columns from definitions not found in the raw table
    for defined_col_name, meta in table_def.items():
        if defined_col_name.lower() not in added_cols:
            col_type = meta.get("schema", {}).get("data_type", "STRING").strip().upper()
            comment = meta.get("description", {}).get("comment", "")
            comment = comment.replace("'", "\\'")
            
            if comment:
                schema_parts.append(f"`{defined_col_name}` {col_type} COMMENT '{comment}'")
            else:
                schema_parts.append(f"`{defined_col_name}` {col_type}")

    return ", ".join(schema_parts)


def build_standard_schema(df: DataFrame, table: str, column_definitions: dict) -> str:
    """
    Dynamically generates a DDL string directly from a DataFrame's current schema.
    Uses definitions for comments but includes ALL columns found in the DF.

    Args:
        df (DataFrame): The DataFrame from which to infer the schema.
        table (str): The name of the table being processed.
        column_definitions (dict): Dictionary mapping columns to their target descriptions.

    Returns:
        str: A DDL string matching the exact DataFrame schema, populated with comments.
    """
    if not column_definitions or table not in column_definitions:
        return None
        
    table_def = column_definitions[table].get("columns", {})
    
    # Pre-compute a lowercase lookup dictionary for O(1) access
    lookup = {k.lower(): v for k, v in table_def.items()}
    
    schema_parts = []
    
    # Iterate through the ACTUAL columns in the DataFrame
    for field in df.schema:
        col_name = field.name
        # Get the type from Spark's inference (guarantees compatibility)
        spark_type = field.dataType.simpleString().upper()
        
        # O(1) case-insensitive lookup
        col_meta = lookup.get(col_name.lower())
        
        if col_meta:
            comment = col_meta.get("description", {}).get("comment") or col_meta.get("comment", "")
            comment = str(comment).replace("'", "\\'") if comment else ""
        else:
            comment = ""
        
        # Append to DDL with or without the comment clause
        if comment:
            schema_parts.append(f"`{col_name}` {spark_type} COMMENT '{comment}'")
        else:
            schema_parts.append(f"`{col_name}` {spark_type}")
            
    return ", ".join(schema_parts)
    

def build_scd_schema(table: str, column_definitions: dict) -> str:
    """
    Constructs a DDL schema string specifically for SCD Type 2 tables using only 
    the static dictionary (bypassing dynamic reads to avoid DLT graph compilation errors).
    Automatically appends native Databricks SCD2 tracking columns.

    Args:
        table (str): The name of the table being processed.
        column_definitions (dict): Dictionary containing target data types and comments.

    Returns:
        str: A formatted DDL string for SCD2 tables, or None if definitions are missing.
    """
    column_definitions = column_definitions or {}
    table_def = column_definitions.get(table, {}).get("columns", {})

    if not table_def:
        print(f"WARNING: Table '{table}' missing from definitions. SCD table will lack comments.")
        return None 

    schema_to_apply = []
    
    for col_name, col_data in table_def.items():
        data_type = col_data.get("schema", {}).get("data_type", "STRING")
        comment = col_data.get("description", {}).get("comment", "").replace("'", "\\'")
        
        if comment:
            schema_to_apply.append(f"`{col_name}` {data_type} COMMENT '{comment}'")
        else:
            schema_to_apply.append(f"`{col_name}` {data_type}")

    # Append Databricks native SCD2 tracking columns
    schema_to_apply.append('__START_AT TIMESTAMP COMMENT "When this record became live"')
    schema_to_apply.append('__END_AT TIMESTAMP COMMENT "When this record became no longer valid"')
    
    return ", ".join(schema_to_apply)


def add_null_check_flags(df, match_keys):
    """
    Evaluates required columns (like Primary Keys) and appends binary 
    flag columns (`DQ_hard_null_...`) indicating if null values are present.

    Args:
        df (DataFrame): The input PySpark DataFrame.
        match_keys (list): List of column names designated as required/non-null.

    Returns:
        DataFrame: Transformed DataFrame with appended null-check flags.
    """
    if not match_keys:
        return df
    
    unique_keys = list(set(match_keys))
    new_col_names = {f"{DQ_HARD_PREFIX}null_{item}" for item in unique_keys}
    
    flag_cols = [
        f.when(df[item].isNull(), 1).otherwise(0).alias(f"{DQ_HARD_PREFIX}null_{item}")
        for item in unique_keys
    ]

    existing_cols_to_keep = [col for col in df.columns if col not in new_col_names]
    return df.select(*existing_cols_to_keep, *flag_cols)


def apply_hard_check_flags(df, table_rules, table_prefix=""):
    """
    Evaluates the DataFrame against custom SQL expressions and appends
    binary flag columns indicating if the rules are breached.
    """
    if not table_rules:
        return df

    new_col_names = {f"{DQ_HARD_PREFIX}{rule_name}" for rule_name in table_rules.keys()}

    flag_cols = []
    for rule_name, rule_logic in table_rules.items():
        formatted_logic = rule_logic.format(PREFIX=table_prefix)
        
        flag_cols.append(
            f.expr(f"CASE WHEN {formatted_logic} THEN 1 else 0 END").alias(
                f"{DQ_HARD_PREFIX}{rule_name}"
            )
        )

    existing_cols_to_keep = [col for col in df.columns if col not in new_col_names]
    return df.select(*existing_cols_to_keep, *flag_cols)


def build_dup_count_df(df: DataFrame, key_cols: list[str]) -> DataFrame:
    """
    Aggregates a streaming DataFrame by specified keys to count row occurrences.
    Used for duplicate detection. (Requires a watermark to be set on the DF).

    Args:
        df (DataFrame): The input streaming PySpark DataFrame.
        key_cols (list[str]): Columns to group by.

    Returns:
        DataFrame: DataFrame containing the keys and a `dup_count` column.
    """
    return (
        df.groupBy(*key_cols)
          .count()
          .withColumnRenamed("count", "dup_count")
    )


def process_sql_left_joins(
    spark, df, sql_checklist, table_config, table, table_prefix=""
):
    """
    Performs broadcast left-joins against SQL reference queries. If a record in the 
    main DataFrame has no match in the reference data, it appends a binary flag column 
    (`DQ_hard_...` or `DQ_soft_...`).

    Args:
        spark (SparkSession): The active Spark session.
        df (DataFrame): The input PySpark DataFrame.
        sql_checklist (dict): Dictionary of predefined SQL reference queries.
        table_config (dict): Join conditions and severity specifications.
        table (str): The name of the table being processed.
        table_prefix (str): Optional database/schema prefix for formatting SQL queries.

    Returns:
        DataFrame: Transformed DataFrame with appended join check flags.
    """
    checks_field = "sql_join_checks"

    if checks_field not in table_config.get(table, {}):
        return df
    
    for check_type, check_config in table_config[table][checks_field].items():
        try:
            if "hard_check" not in check_config:
                raise ValueError(
                    f"Missing 'hard_check' flag for check '{check_type}' in table '{table}'."
                )

            sql_query = sql_checklist[check_type].format(PREFIX=table_prefix)
            try:
                sql_df = spark.sql(sql_query)
            except Exception as e:
                raise RuntimeError(f"Failed to execute SQL for check '{check_type}': {e}")

            join_keys_mapping = {k: v for k, v in check_config.items() if k != "hard_check"}
            left_keys = list(join_keys_mapping.keys())
            right_keys = list(join_keys_mapping.values())

            missing_cols = set(right_keys) - set(sql_df.columns)
            if missing_cols:
                raise ValueError(
                    f"SQL Query for `{check_type}` is missing required columns: {missing_cols}"
                )

            select_exprs = []
            join_conditions = []

            for l_key, r_key in join_keys_mapping.items():
                r_key_aliased = f"__ref_{r_key}"
                select_exprs.append(f.col(r_key).alias(r_key_aliased))
                join_conditions.append(f.col(l_key) == f.col(r_key_aliased))
            
            ref_df = sql_df.select(*select_exprs).distinct()

            is_hard = check_config["hard_check"]
            DQ_PREFIX = DQ_HARD_PREFIX if is_hard else DQ_SOFT_PREFIX
            flag_col_name = f"{DQ_PREFIX}{check_type}"

            joined_df = df.join(
                f.broadcast(ref_df), 
                on=join_conditions, 
                how="left"
            )

            check_col = f"__ref_{right_keys[0]}"
            joined_df = joined_df.withColumn(
                flag_col_name,
                f.when(f.col(check_col).isNull(), 1).otherwise(0)
            )

            cols_to_drop = [f"__ref_{k}" for k in right_keys]
            df = joined_df.drop(*cols_to_drop)

        except Exception as e:
            print(f"❌ Error processing check `{check_type}`: {e}")
            raise e

    return df


def check_sql_reference_mismatches(spark, df, sql_queries, table_config, table):
    """
    Performs left-anti joins to identify records that exist in the reference datasets 
    but are missing from the main DataFrame. Aggregates results into a summary payload.

    Args:
        spark (SparkSession): The active Spark session.
        df (DataFrame): The main DataFrame being validated.
        sql_queries (dict): Dictionary of SQL reference queries.
        table_config (dict): Check configurations and column mappings.
        table (str): The name of the table being processed.

    Returns:
        DataFrame: A summary DataFrame detailing missing reference values, counts, and check names.
    """
    empty_schema = StructType(
        [
            StructField("validation_check_name", StringType(), True),
            StructField("count", IntegerType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("column_names", StringType(), True),
            StructField("missing_values", StringType(), True),
        ]
    )

    checks_field = "sql_left_anti_join_checks"
    if checks_field not in table_config.get(table, {}):
        return spark.createDataFrame([], empty_schema)

    checks_config = table_config[table][checks_field]
    dfs_to_union = []

    for validation_check_name, column_mapping in checks_config.items():
        sql_query = sql_queries.get(validation_check_name)
        if not sql_query:
            raise ValueError(f"❌ Check `{validation_check_name}` not found in imported SQL queries.")

        if not column_mapping or not isinstance(column_mapping, dict):
            raise ValueError(f"❌ No valid join conditions for `{validation_check_name}` in `{checks_field}`.")

        try:
            sql_df = spark.sql(sql_query)
        except Exception as e:
            raise Exception(f"❌ Error executing SQL query for `{validation_check_name}`: {e}")
        
        required_sql_cols = set(column_mapping.values())
        required_df_cols = set(column_mapping.keys())

        if not required_sql_cols.issubset(set(sql_df.columns)):
            raise ValueError(f"Missing cols in SQL {validation_check_name}")

        sql_df_clean = sql_df.select(*required_sql_cols).distinct()

        renamed_df = df.select(
            *[df[col].alias(f"{col}_source") for col in required_df_cols]
        ).distinct()

        join_conditions = [
            renamed_df[f"{key}_source"] == sql_df_clean[val]
            for key, val in column_mapping.items()
        ]

        unmatched_rows_df = sql_df_clean.alias("sql").join(
            renamed_df, on=join_conditions, how="leftanti"
        )

        column_names_str = " | ".join(required_sql_cols)

        summary_df = unmatched_rows_df.groupBy(
            f.lit(validation_check_name).alias("validation_check_name")
        ).agg(
            f.count("*").alias("count"),
            f.current_timestamp().alias("timestamp"),
            f.lit(column_names_str).alias("column_names"),
            f.collect_list(
                f.concat_ws(" | ", *[f.col(col) for col in sql_df.columns])
            ).alias("missing_values")
        ).withColumn(
            "missing_values", f.concat_ws(", ", f.col("missing_values"))
        )

        dfs_to_union.append(summary_df)
    
    if dfs_to_union:
        final_df = reduce(DataFrame.union, dfs_to_union)
    else:
        final_df = spark.createDataFrame([], empty_schema)

    return final_df


def filter_quarantine_clean(df: DataFrame) -> (DataFrame, DataFrame):
    """
    Reads all DQ flags on a DataFrame and splits it into two distinct DataFrames:
    one containing isolated failed records, and another containing only clean records.

    Args:
        df (DataFrame): The evaluated DataFrame containing data quality columns (`DQ_...`).

    Returns:
        tuple:
            - DataFrame: Quarantine DF containing records that failed any hard or soft checks.
            - DataFrame: Clean DF containing records passing all hard & soft checks (DQ columns dropped).
    """
    dq_hard_cols = [col for col in df.columns if col.startswith(DQ_HARD_PREFIX)]
    dq_soft_cols = [col for col in df.columns if col.startswith(DQ_SOFT_PREFIX)]

    if len(dq_hard_cols) > 1:
        df = df.withColumn(
            f"{DQ_HARD_PREFIX}quarantine", f.greatest(*[f.col(c) for c in dq_hard_cols])
        )
    elif len(dq_hard_cols) == 1:
        df = df.withColumn(f"{DQ_HARD_PREFIX}quarantine", f.col(dq_hard_cols[0]))
    else:
        df = df.withColumn(f"{DQ_HARD_PREFIX}quarantine", f.lit(0)) 

    if len(dq_soft_cols) > 1:
        df = df.withColumn(
            f"{DQ_SOFT_PREFIX}quarantine", f.greatest(*[f.col(c) for c in dq_soft_cols])
        )
    elif len(dq_soft_cols) == 1:
        df = df.withColumn(f"{DQ_SOFT_PREFIX}quarantine", f.col(dq_soft_cols[0]))
    else:
        df = df.withColumn(f"{DQ_SOFT_PREFIX}quarantine", f.lit(0)) 

    quarantine_df = df.filter(
        (f.col(f"{DQ_HARD_PREFIX}quarantine") == 1)
        | (f.col(f"{DQ_SOFT_PREFIX}quarantine") == 1)
    )

    clean_df = df.filter(
        (f.col(f"{DQ_HARD_PREFIX}quarantine") == 0)
    )

    cols_to_drop = [col for col in clean_df.columns if col.startswith("DQ_")]
    clean_df = clean_df.drop(*cols_to_drop)

    return quarantine_df, clean_df