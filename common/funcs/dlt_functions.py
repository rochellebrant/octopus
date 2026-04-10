import sys
import dlt
import importlib
import os
from pyspark.sql import SparkSession

from pyspark.sql import DataFrame, functions as f
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)
from pyspark.sql.functions import col
from functools import reduce
import dlt_helper

DQ_HARD_PREFIX = "DQ_hard_"
DQ_SOFT_PREFIX = "DQ_soft_"


def unpack_api_landing_data(spark, table, input_prefix, output_prefix, api_mapping):
    """
    Flattens raw JSON/Variant API payloads, extracts required columns, and generates a row hash.

    Args:
        spark (SparkSession): The active Spark session.
        table (str): The name of the table being processed.
        input_prefix (str): Prefix of the source table.
        output_prefix (str): Prefix for the target table.
        api_mapping (dict): Dictionary mapping raw API field names to clean column names.
    """
    table_name = f"{output_prefix}{table}"
    
    @dlt.table(
        name=table_name,
        comment=f"Unpacked and flattened JSON payload for {table}",
        temporary=True 
    )
    def get_unpacked_data():
        df = spark.readStream.option("skipChangeCommits", "true").table(f"{input_prefix}{table}")
        
        # 1. Explode the variant array
        exploded_df = df.select(
            "*",
            f.explode(f.expr("cast(raw_data:result as array<variant>)")).alias("record")
        )
        
        # 2. Dynamically build the select statement based on your API_FIELD_MAPPING
        cols_to_extract = []
        for clean_col_name, api_field in api_mapping[table].items():
            # Extract the ugly API field and rename it to the clean column name
            cols_to_extract.append(
                f.expr(f"cast(record:{api_field} as string)").alias(clean_col_name)
            )
            
        # Keep our metadata
        metadata_cols = [f.col("table_id"), f.col("datalake_ingestion_timestamp")]
        flattened_df = exploded_df.select(*(metadata_cols + cols_to_extract))
        
        # 3. Generate the row hash for SCD2 audit tracking
        cols_to_hash = [c for c in flattened_df.columns if c not in ["datalake_ingestion_timestamp"]]
        concat_expr = f.concat_ws("||", *[f.col(c).cast("string") for c in cols_to_hash])
        final_df = flattened_df.withColumn("row_hash", f.sha2(concat_expr, 256))
        
        return final_df


def deduplicate_raw_stream(spark, table, input_prefix, output_prefix, match_keys):
    """
    Removes duplicate records from a streaming table based on specified match keys.

    Args:
        spark (SparkSession): The active Spark session.
        table (str): The name of the table being processed.
        input_prefix (str): Prefix of the source table.
        output_prefix (str): Prefix for the target table.
        match_keys (list): List of column names used to identify exact duplicates.
    """
    table_name = f"{output_prefix}{table}"

    @dlt.table(
        name=table_name,
        comment=f"Deduplicated raw stream for {table}",
        temporary=True 
    )
    def deduplicate_stream():
        return (
            spark.readStream.table(f"live.{input_prefix}{table}")
            .dropDuplicates(match_keys) # Now accepts a list of columns!
        )


def create_table(
    spark, table, input_prefix, output_prefix,
    temporary=False, comment=None, apply_casting=False,
    column_definitions=None, table_config=None,
    schema_source_path=None,
):
    """
    Registers a new DLT table, optionally applying schema definitions and casting data types.
    """
    table_name = f"{output_prefix}{table}"
    
    # Smart prefixing: if input_prefix contains a '.', it's an external UC table.
    # Otherwise, it's an internal pipeline table that needs the 'live.' prefix.
    if "." in input_prefix:
        input_table_path = f"{input_prefix}{table}"
    else:
        input_table_path = f"live.{input_prefix}{table}"
    
    # --- BULLETPROOF SCHEMA LOGIC ---
    final_schema = None
    if column_definitions:
        # If no explicit external source path is provided, it is an internal pipeline 
        # table. NEVER read it during planning. Always use the static builder.
        if not schema_source_path:
            final_schema = dlt_helper.build_static_schema(table, column_definitions)
        else:
            # Only read dynamically if the user explicitly provided an external source path
            final_schema = dlt_helper.build_live_schema(
                spark, schema_source_path, table, column_definitions
            )

    dict_table_comment = column_definitions.get(table, {}).get("table", {}).get("comment") if column_definitions else None
    final_table_comment = comment or dict_table_comment or f"Source: {input_prefix}{table}"

    @dlt.table(
        name=table_name,
        comment=final_table_comment,
        temporary=temporary,
        schema=final_schema
    )
    def get_table():
        df = spark.readStream.option("skipChangeCommits", "true").table(input_table_path)
        
        if apply_casting and column_definitions and table_config:
            df = dlt_helper.cast_columns(df, column_definitions, table_config, table)
            # Re-apply comments to the underlying DataFrame metadata so they persist cleanly
            df = dlt_helper.apply_column_comments(df, table, column_definitions)
            
        return df


def validate_missing_references(
    spark: SparkSession, table: str, sql_queries, table_config, input: str, output: str
):
    """
    Identifies records missing from reference tables by performing an anti-join against SQL queries.

    Args:
        spark (SparkSession): The active Spark session.
        table (str): The name of the table being processed.
        sql_queries (dict): Pre-defined SQL queries acting as reference datasets.
        table_config (dict): Configuration mapping check conditions.
        input (str): Source table prefix.
        output (str): Target table prefix.
    """

    @dlt.table(
        name=f"{output}{table}",
        comment=f"Summary of records in the reference tables that do not exist in the fetched data.",
        temporary=False,
    )
    def generate_unmatched_reference_data_summary():
        df = spark.read.table(f"live.{input}{table}")
        unmatched_df = dlt_helper.check_sql_reference_mismatches(
            spark, df, sql_queries, table_config, table
        )
        return unmatched_df


def apply_null_col_checks(spark, table, table_config, input, output):
    """
    Appends binary flag columns indicating if a row has missing values in primary keys or required columns.
    """
    # default columns to check
    cols_to_check = [item for item in table_config[table].get("match_keys", [])]

    if "non_null_columns" in table_config[table]:
        cols_to_check.extend(table_config[table]["non_null_columns"])

    # column exemptions in the config (defaults to an empty list)
    exempt_cols = table_config[table].get("exempt_from_null_checks", [])
    
    # filter out the exempt columns and deduplicate
    final_cols_to_check = list(set([c for c in cols_to_check if c not in exempt_cols]))

    @dlt.table(
        name=f"{output}{table}",
        comment=f"Intermediate table for {table}, with flags indicating null values in match key fields (e.g. PKs) and non-null columns defined in the configuration.",
        temporary=True,
    )
    def get_null_flag_table():
        df = spark.readStream.table(f"live.{input}{table}")
        # Pass the filtered list to your helper
        df = dlt_helper.add_null_check_flags(df, final_cols_to_check)
        return df


def apply_sql_left_join_checks(
    spark, table, sql_checklist, table_config, input, output, table_prefix=""
):
    """
    Appends binary flag columns indicating if a row fails to map against a SQL reference query.

    Args:
        spark (SparkSession): The active Spark session.
        table (str): The name of the table being processed.
        sql_checklist (dict): SQL reference queries to validate against.
        table_config (dict): Settings detailing the join conditions and rule severities.
        input (str): Source table prefix.
        output (str): Target table prefix.
        table_prefix (str): Optional database/schema prefix for SQL checks.
    """

    @dlt.table(
        name=f"{output}{table}",
        comment=f"Intermediate table for {table}, adding flags to indicate records with values that do not align with the user-defined SQL checks.",
        temporary=True,
    )
    def create_sql_check_flags():
        df = spark.readStream.table(f"live.{input}{table}")
        df = dlt_helper.process_sql_left_joins(
            spark, df, sql_checklist, table_config, table, table_prefix
        )
        return df


def apply_custom_hard_checks(spark, table, table_rules, input, output, table_prefix=""):
    """
    Appends binary flag columns evaluating the row against custom SQL-based data quality rules.

    Args:
        spark (SparkSession): The active Spark session.
        table (str): The name of the table being processed.
        table_rules (dict): Dictionary mapping rule names to their SQL logic expressions.
        input (str): Source table prefix.
        output (str): Target table prefix.
    """

    @dlt.table(
        name=f"{output}{table}",
        comment=f"Intermediate table for {table}, adding columns that flag records based on custom hard data quality rules defined in the configuration.",
        temporary=True,
    )
    def create_hard_rule_flag():
        df = spark.readStream.table(f"live.{input}{table}")
        rule = table_rules.get(table)
        if rule:
            df = dlt_helper.apply_hard_check_flags(df, rule, table_prefix=table_prefix)
        return df


def enforce_uniqueness_expectations(
    spark,
    table: str,
    table_config: dict,
    input: str,
    output: str,
):
    """
    Creates duplicate-checking pipelines using DLT expectations. Fails the batch if duplicates are detected.

    Args:
        spark (SparkSession): The active Spark session.
        table (str): The name of the table being processed.
        table_config (dict): Configuration containing uniqueness logic (composite/individual keys).
        input (str): Source table prefix.
        output (str): Target table prefix.
    """
    uniq_conf = table_config.get(table, {}).get("uniqueness_expectations")

    # 1 · No expectations ⇒ simple pass-through table
    if not uniq_conf:
        @dlt.table(
            name=f"{output}{table}",
            comment="Pass-through; no uniqueness expectations configured.",
            temporary=True,
        )
        def passthrough():
            return spark.readStream.table(f"live.{input}{table}")

        return

    # 2 · Expectation-backed duplicate check
    load_ts = table_config[table]["load_date_column"]
    individual = uniq_conf.get("individual", [])
    composite = uniq_conf.get("composite", [])

    @dlt.table(
        name=f"{output}{table}_dupcheck",
        comment="Aggregated duplicate counts per key set; batch fails if any dup_count > 1.",
        temporary=False,
    )
    @dlt.expect_or_fail("unique_keys", "dup_count = 1")
    def dup_check():
        src = (
            spark.readStream.table(f"live.{input}{table}")
            .withWatermark(load_ts, "20 minutes")
        )

        all_checks: list[DataFrame] = []

        for col in individual:
            df_ind = (
                dlt_helper.build_dup_count_df(src, [col])
                .withColumn("check_name", f.lit(f"ind__{col}"))
            )
            all_checks.append(df_ind)

        for combo in composite:
            alias = "_".join(combo)
            df_cmp = (
                dlt_helper.build_dup_count_df(src, combo)
                .withColumn("check_name", f.lit(f"cmp__{alias}"))
            )
            all_checks.append(df_cmp)

        if not all_checks:
            raise ValueError(
                f"{table}: uniqueness_expectations provided but no keys defined."
            )

        if len(all_checks) == 1:
            return all_checks[0]

        return reduce(
            lambda x, y: x.unionByName(y, allowMissingColumns=True), all_checks
        )

    # 3 · Pass-through of original rows (executes only if dup_check passes)
    @dlt.table(
        name=f"{output}{table}",
        comment="Original bronze rows; available only when duplicate check passes.",
        temporary=True,
    )
    def unique_pass():
        return spark.readStream.table(f"live.{input}{table}")
    

def quarantine_layer(
    spark: SparkSession,
    table: str,
    input: str,
    output_quarantine: str,
    output_clean: str,
):
    """
    Splits evaluated data into two DLT tables: 'clean' (passed checks) and 'quarantine' (failed checks).

    Args:
        spark (SparkSession): The active Spark session.
        table (str): The name of the table being processed.
        input (str): Source table prefix.
        output_quarantine (str): Target table prefix for failed records.
        output_clean (str): Target table prefix for passed records.
    """

    @dlt.table(
        name=f"{output_quarantine}{table}",
        comment=f"Records flagged for quarantine based on hard or soft data quality rules.",
    )
    def landing_quarantine():
        df = spark.readStream.table(f"live.{input}{table}")
        quarantine_df, _ = dlt_helper.filter_quarantine_clean(df)
        return quarantine_df

    @dlt.table(
        name=f"{output_clean}{table}",
        comment=f"Cleaned records that pass all hard data quality checks.",
        temporary=True,
    )
    def landing_clean():
        df = spark.readStream.table(f"live.{input}{table}")
        _, clean_df = dlt_helper.filter_quarantine_clean(df)
        return clean_df


def generate_scd_tables(
    spark,
    source_table_prefix,
    table,
    table_config,
    column_defintions,
    input,
    output,
    defined_schema=True,
):
    """
    Implements Slowly Changing Dimensions (SCD) Type 2 logic using dlt.apply_changes to track history.

    Args:
        spark (SparkSession): The active Spark session.
        source_table_prefix (str): Contextual prefix for descriptions.
        table (str): The name of the table being processed.
        table_config (dict): Contains match keys, audit keys, and load sequence columns.
        column_defintions (dict): Definitions for injecting table and column comments.
        input (str): Source table prefix.
        output (str): Target table prefix.
        defined_schema (bool): Applies the DDL schema if True.
    """
    load_date_key = table_config[table]["load_date_column"]

    match_keys = [
        item for item in table_config[table]["match_keys"] if item != load_date_key
    ]

    history_keys = (
        table_config[table].get("audit_keys", []) + [load_date_key]
        if "audit_keys" in table_config[table]
        else None
    )

    new_schema = (
        dlt_helper.build_scd_schema(
            table, column_defintions
        )
        if defined_schema == True
        else None
    )

    dict_table_comment = None
    if column_defintions and table in column_defintions:
        dict_table_comment = column_defintions[table].get("table", {}).get("comment")
        
    final_comment = dict_table_comment or f"Table for {source_table_prefix}{table} implementing Slowly Changing Dimensions (SCD) logic."

    dlt.create_streaming_table(
        name=f"{output}{table}",
        comment=final_comment,
        schema=new_schema,
    )

    dlt.apply_changes(
        target=f"{output}{table}",
        source=f"{input}{table}",
        keys=match_keys,
        sequence_by=load_date_key,
        stored_as_scd_type="2",
        track_history_except_column_list=history_keys,
        ignore_null_updates=True,
    )


def generate_unioned_materialised_view(spark, table, table_config, input, output):
    """
    Appends the pipeline's incoming data to an existing unified materialized view.

    Args:
        spark (SparkSession): The active Spark session.
        table (str): The name of the table being processed.
        table_config (dict): General configuration dictionary.
        input (str): Source table prefix.
        output (str): Target view name.
    """

    @dlt.append_flow(target=f"{output}", name=f"unifying_materialised_view_{table}")
    def unified_table():
        df = spark.read.table(f"live.{input}{table}")
        return df


def generate_unioned_quarantine_table(spark, table, table_config, input, output):
    """
    LEGACY: Groups quarantine rule flags by time window to create an aggregated view of data issues.

    Args:
        spark (SparkSession): The active Spark session.
        table (str): The name of the table being processed.
        table_config (dict): Contains the 'load_date_column' for watermarking.
        input (str): Source table prefix.
        output (str): Target unified table name.
    """
    load_column = table_config[table]["load_date_column"]
    if not load_column:
        raise ValueError(f"Missing 'load_date_column' for table '{table}' in table_config.")

    @dlt.append_flow(target=f"{output}", name=f"append_flow_to_union_{table}")
    def create_single_quarantine_table():
        input_table_path = f"live.{input}{table}"
        stream_df = spark.readStream.table(input_table_path)

        df = (
            (stream_df.withWatermark(load_column, "120 minutes"))
            .groupBy(f.window(load_column, "120 minutes").alias("timestamp_window"))
            .agg(
                f.count("*").alias("count"), 
                f.sum(f.when(f.col(f"{DQ_HARD_PREFIX}quarantine") == 1, 1).otherwise(0)).alias("hard_quarantine"),
                f.sum(f.when(f.col(f"{DQ_SOFT_PREFIX}quarantine") == 1, 1).otherwise(0)).alias("soft_quarantine"),
            )
            .withColumn("table", f.lit(f"{table}"))
            .withColumn("timestamp", f.to_timestamp(f.col("timestamp_window.start")))
            .drop("timestamp_window")
        )
        return df


def generate_raw_quarantine_log(spark, table, table_config, input, output):
    """
    Packs varied quarantine table schemas into JSON strings and funnels them into a single master log.

    Args:
        spark (SparkSession): The active Spark session.
        table (str): The name of the table being processed.
        table_config (dict): Configuration mapping the load date column.
        input (str): Source table prefix.
        output (str): Target master log table name.
    """
    load_column = table_config[table]["load_date_column"]

    @dlt.append_flow(target=output, name=f"append_quarantine_raw_{table}")
    def append_raw_quarantine():
        df = (spark.readStream
              .option("skipChangeCommits", "true") 
              .table(f"live.{input}{table}"))
        unified_df = df.select(
            f.lit(table).alias("source_table"),
            f.col(load_column).alias("ingestion_timestamp"),
            f.to_json(f.struct(f.col("*"))).alias("quarantined_record_json")
        )
        return unified_df