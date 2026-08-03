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


def unpack_api_landing_data(
    spark, table, input_prefix, output_prefix, api_mapping, temporary=True
):
    table_name = f"{output_prefix}{table}"

    @dlt.table(
        name=table_name,
        comment=f"Unpacked JSON with Tombstone injection for {table}",
        temporary=temporary,
    )
    def get_unpacked_data():
        # Process API stream
        df = spark.readStream.table(f"{input_prefix}{table}")
        exploded_df = df.select(
            "*",
            f.explode(f.expr("cast(raw_data:result as array<variant>)")).alias(
                "record"
            ),
        )

        mapped_df = exploded_df.withColumn(
            "record_map",
            f.from_json(f.col("record").cast("string"), "map<string,string>"),
        )

        cols_to_extract = []
        for clean_col_name, api_field in api_mapping[table].items():
            # Use map_filter to find the key that ENDS with the ID, ignoring the dynamic prefix
            extract_expr = f"element_at(map_values(map_filter(record_map, (k, v) -> k LIKE '%{api_field}')), 1)"
            cols_to_extract.append(f.expr(extract_expr).alias(clean_col_name))

        metadata_cols = [f.col("table_id"), f.col("datalake_ingestion_timestamp")]

        # Add the is_deleted flag defaulting to False for all normal active API rows
        normal_stream = mapped_df.select(*(metadata_cols + cols_to_extract)).withColumn(
            "is_deleted", f.lit(False)
        )

        # Process tombstone stream
        # Wrapped in a try/except so DLT doesn't crash on Day 1 before tombstone tables exist
        try:
            tombstone_stream = spark.readStream.table(
                f"{input_prefix}tombstones_{table}"
            )
            # Union the active stream with the deleted stream
            final_stream = normal_stream.unionByName(
                tombstone_stream, allowMissingColumns=True
            )
        except Exception as e:
            print(
                f"No tombstone table found for {table}. Proceeding with active stream only."
            )
            final_stream = normal_stream

        # Apply row hashing
        # Exclude our system columns from the hash so they don't trigger false updates
        cols_to_hash = [
            c
            for c in final_stream.columns
            if c not in ["datalake_ingestion_timestamp", "is_deleted"]
        ]
        concat_expr = f.concat_ws(
            "||", *[f.col(c).cast("string") for c in cols_to_hash]
        )

        return final_stream.withColumn("row_hash", f.sha2(concat_expr, 256))


def deduplicate_raw_stream(
    spark, table, input_prefix, output_prefix, match_keys, temporary=True
):
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
        temporary=temporary,
    )
    def deduplicate_stream():
        input_path = (
            f"{input_prefix}{table}"
            if "." in input_prefix
            else f"live.{input_prefix}{table}"
        )
        return spark.readStream.table(input_path).dropDuplicates(match_keys)


def create_table(
    spark,
    table,
    input_prefix,
    output_prefix,
    temporary=False,
    comment=None,
    apply_casting=False,
    column_definitions=None,
    table_config=None,
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

    # --- DYNAMICALLY INJECT SCHEMA ONLY FOR OPT-IN PIPELINES ---
    if final_schema and table_config and table in table_config:
        delete_expr = table_config[table].get("apply_as_deletes_expr")
        if (
            delete_expr
            and "is_deleted" in delete_expr
            and "is_deleted" not in final_schema.lower()
        ):
            final_schema += (
                ", `is_deleted` BOOLEAN COMMENT 'Internal DLT flag for SCD2 deletes'"
            )
    # --------------------------------------------------------------------

    dict_table_comment = (
        column_definitions.get(table, {}).get("table", {}).get("comment")
        if column_definitions
        else None
    )
    final_table_comment = (
        comment or dict_table_comment or f"Source: {input_prefix}{table}"
    )

    @dlt.table(
        name=table_name,
        comment=final_table_comment,
        temporary=temporary,
        schema=final_schema,
    )
    def get_table():
        df = spark.readStream.option("skipChangeCommits", "true").table(
            input_table_path
        )

        # --- DYNAMICALLY UNION TOMBSTONES ONLY FOR OPT-IN PIPELINES ---
        delete_expr = (
            table_config.get(table, {}).get("apply_as_deletes_expr")
            if table_config
            else None
        )

        if delete_expr and "is_deleted" in delete_expr:
            # Add the active flag so the schema matches the tombstones
            if "is_deleted" not in df.columns:
                df = df.withColumn("is_deleted", f.lit(False))

            try:
                # Dynamically construct the tombstone table path using the same prefix logic
                if "." in input_prefix:
                    tombstone_path = f"{input_prefix}tombstones_{table}"
                else:
                    tombstone_path = f"live.{input_prefix}tombstones_{table}"

                tombstone_stream = spark.readStream.option(
                    "skipChangeCommits", "true"
                ).table(tombstone_path)
                df = df.unionByName(tombstone_stream, allowMissingColumns=True)
            except Exception as e:
                # Fail gracefully if the pipeline is Day 1 or if no tombstones were generated today
                pass
        # --------------------------------------------------------------

        if apply_casting and column_definitions and table_config:
            df = dlt_helper.cast_columns(df, column_definitions, table_config, table)
            # Re-apply comments to the underlying DataFrame metadata so they persist cleanly
            df = dlt_helper.apply_column_comments(df, table, column_definitions)

        return df


def validate_missing_references(
    spark: SparkSession,
    table: str,
    sql_queries,
    table_config,
    input: str,
    output: str,
    temporary=True,
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
        temporary=temporary,
    )
    def generate_unmatched_reference_data_summary():
        input_path = f"{input}{table}" if "." in input else f"live.{input}{table}"
        df = spark.read.table(input_path)

        unmatched_df = dlt_helper.check_sql_reference_mismatches(
            spark, df, sql_queries, table_config, table
        )
        return unmatched_df


def apply_null_col_checks(spark, table, table_config, input, output, temporary=True):
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
        temporary=temporary,
    )
    def get_null_flag_table():
        input_path = f"{input}{table}" if "." in input else f"live.{input}{table}"
        df = spark.readStream.table(input_path)
        df = dlt_helper.add_null_check_flags(df, final_cols_to_check)
        return df


def apply_sql_left_join_checks(
    spark,
    table,
    sql_checklist,
    table_config,
    input,
    output,
    table_prefix="",
    temporary=True,
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
        temporary=temporary,
    )
    def create_sql_check_flags():
        input_path = f"{input}{table}" if "." in input else f"live.{input}{table}"
        df = spark.readStream.table(input_path)
        df = dlt_helper.process_sql_left_joins(
            spark, df, sql_checklist, table_config, table, table_prefix
        )
        return df


def apply_custom_hard_checks(
    spark,
    table,
    table_rules,
    input,
    output,
    table_prefix="",
    landing_db="",
    bronze_db="",
    silver_db="",
    temporary=True,
):
    """
    Appends binary flag columns evaluating the row against custom SQL-based data quality rules.

    Args:
        spark (SparkSession): The active Spark session.
        table (str): The name of the table being processed.
        table_rules (dict): Dictionary mapping rule names to their SQL logic expressions.
        input (str): Source table prefix.
        output (str): Target table prefix.
        landing_db (str): Fully-qualified landing schema, for rules that reference it directly.
        bronze_db (str): Fully-qualified bronze schema, for rules that reference it directly.
        silver_db (str): Fully-qualified silver schema, for rules that reference it directly.
    """

    @dlt.table(
        name=f"{output}{table}",
        comment=f"Intermediate table for {table}, adding columns that flag records based on custom hard data quality rules defined in the configuration.",
        temporary=temporary,
    )
    def create_hard_rule_flag():
        input_path = f"{input}{table}" if "." in input else f"live.{input}{table}"
        df = spark.readStream.table(input_path)
        rule = table_rules.get(table)
        if rule:
            df = dlt_helper.apply_hard_check_flags(
                df,
                rule,
                table_prefix=table_prefix,
                landing_db=landing_db,
                bronze_db=bronze_db,
                silver_db=silver_db,
            )
        return df


def apply_duplicate_check_flags(
    spark, table, table_config, input, output, temporary=True
):
    """
    Appends binary flag columns marking rows that violate the table's uniqueness_expectations,
    so they can be routed to quarantine by quarantine_layer rather than blocking the whole table.

    Args:
        spark (SparkSession): The active Spark session.
        table (str): The name of the table being processed.
        table_config (dict): Configuration containing uniqueness logic (composite/individual keys).
        input (str): Source table prefix.
        output (str): Target table prefix.
    """
    uniq_conf = table_config.get(table, {}).get("uniqueness_expectations")

    @dlt.table(
        name=f"{output}{table}",
        comment=f"Intermediate table for {table}, adding flags for rows that violate uniqueness_expectations.",
        temporary=temporary,
    )
    def create_duplicate_flags():
        input_path = f"{input}{table}" if "." in input else f"live.{input}{table}"
        df = spark.readStream.table(input_path)

        if not uniq_conf:
            return df

        static_df = spark.table(input_path)

        for key_col in uniq_conf.get("individual", []):
            df = dlt_helper.flag_duplicates(
                df, static_df, [key_col], f"{DQ_HARD_PREFIX}duplicate_{key_col}"
            )

        for combo in uniq_conf.get("composite", []):
            alias = "_".join(combo)
            df = dlt_helper.flag_duplicates(
                df, static_df, combo, f"{DQ_HARD_PREFIX}duplicate_{alias}"
            )

        return df


def enforce_uniqueness_expectations(
    spark,
    table: str,
    table_config: dict,
    input: str,
    output: str,
):
    """
    Creates a persisted duplicate-count table backed by a DLT expectation, so the pipeline
    update still fails when duplicates are present. Does not gate the clean/quarantine flows —
    see apply_duplicate_check_flags for the flagging that actually routes duplicate rows to quarantine.

    Args:
        spark (SparkSession): The active Spark session.
        table (str): The name of the table being processed.
        table_config (dict): Configuration containing uniqueness logic (composite/individual keys).
        input (str): Source table prefix.
        output (str): Target table prefix.
    """
    uniq_conf = table_config.get(table, {}).get("uniqueness_expectations")

    # No expectations configured ⇒ nothing to enforce
    if not uniq_conf:
        return

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
        input_path = f"{input}{table}" if "." in input else f"live.{input}{table}"
        src = spark.readStream.table(input_path).withWatermark(load_ts, "20 minutes")

        all_checks: list[DataFrame] = []

        for col in individual:
            df_ind = dlt_helper.build_dup_count_df(src, [col]).withColumn(
                "check_name", f.lit(f"ind__{col}")
            )
            all_checks.append(df_ind)

        for combo in composite:
            alias = "_".join(combo)
            df_cmp = dlt_helper.build_dup_count_df(src, combo).withColumn(
                "check_name", f.lit(f"cmp__{alias}")
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
        input_path = f"{input}{table}" if "." in input else f"live.{input}{table}"
        df = spark.readStream.table(input_path)
        quarantine_df, _ = dlt_helper.filter_quarantine_clean(df)

        if "is_deleted" in quarantine_df.columns:
            quarantine_df = quarantine_df.filter(f.col("is_deleted") == False)

        return quarantine_df

    @dlt.table(
        name=f"{output_clean}{table}",
        comment=f"Cleaned records that pass all hard data quality checks.",
        temporary=True,
    )
    def landing_clean():
        input_path = f"{input}{table}" if "." in input else f"live.{input}{table}"
        df = spark.readStream.table(input_path)

        # --- DIPLOMATIC IMMUNITY FOR TOMBSTONES ---
        # Neutralize any hard DQ failures on tombstone rows so they survive to reach SCD2
        if "is_deleted" in df.columns:
            for c in df.columns:
                if c.startswith("DQ_hard_"):
                    df = df.withColumn(
                        c, f.when(f.col("is_deleted") == True, 0).otherwise(f.col(c))
                    )

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
    """
    load_date_key = table_config[table]["load_date_column"]

    match_keys = [
        item for item in table_config[table]["match_keys"] if item != load_date_key
    ]

    provided_audit_keys = table_config[table].get("audit_keys", [])
    history_exclude_list = list(set(provided_audit_keys + [load_date_key]))

    # Look for the optional delete expression in the pipeline's config
    delete_expr = table_config[table].get("apply_as_deletes_expr")

    new_schema = (
        dlt_helper.build_scd_schema(table, column_defintions)
        if defined_schema == True
        else None
    )

    if new_schema and delete_expr and "is_deleted" in delete_expr:
        new_schema += (
            ", `is_deleted` BOOLEAN COMMENT 'Internal DLT flag for SCD2 deletes'"
        )
        history_exclude_list.append("is_deleted")

    dict_table_comment = None
    if column_defintions and table in column_defintions:
        dict_table_comment = column_defintions[table].get("table", {}).get("comment")

    final_comment = (
        dict_table_comment
        or f"Table for {source_table_prefix}{table} implementing Slowly Changing Dimensions (SCD) logic."
    )

    dlt.create_streaming_table(
        name=f"{output}{table}",
        comment=final_comment,
        schema=new_schema,
    )

    # --- THE FIX: SANITIZE VIEW NAME AND ADD 'LIVE.' PREFIX ---
    # Strip out any dots to prevent the MULTIPART_VIEW_NAME crash
    view_name = f"v_scd_src_{table}".replace(".", "_")

    @dlt.view(name=view_name)
    def get_scd_source():
        # Ensure we are pointing to the correct live pipeline table
        input_table_path = f"{input}{table}" if "." in input else f"live.{input}{table}"
        df = spark.readStream.table(input_table_path)

        if delete_expr and "is_deleted" in delete_expr:
            if "is_deleted" not in df.columns:
                df = df.withColumn("is_deleted", f.lit(False))
        return df

    # ----------------------------------------------------------

    # Build the base arguments dictionary that ALL pipelines use
    apply_changes_kwargs = {
        "target": f"{output}{table}",
        "source": view_name,  # Point this to our cleanly named view
        "keys": match_keys,
        "sequence_by": load_date_key,
        "stored_as_scd_type": "2",
        "track_history_except_column_list": history_exclude_list,
        "ignore_null_updates": True,
    }

    # Dynamically inject the delete logic ONLY if the pipeline configured it
    if delete_expr:
        apply_changes_kwargs["apply_as_deletes"] = delete_expr.replace("True", "true")

    # Unpack the dictionary into the function
    dlt.apply_changes(**apply_changes_kwargs)


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
        input_path = f"{input}{table}" if "." in input else f"live.{input}{table}"
        df = spark.read.table(input_path)
        return df


def generate_unioned_quarantine_table(spark, table, table_config, input, output):
    """
    Aggregates quarantine flag counts for a table into a single snapshot row and appends it
    to a unified quarantine table. Reads the quarantine table as a batch snapshot (not a
    stream) so it works under both triggered/full-refresh batch pipelines and continuously
    running incremental ones: `once=True` means the append only fires again once the pipeline
    is fully refreshed, avoiding duplicate snapshot rows on unchanged incremental re-runs.

    Args:
        spark (SparkSession): The active Spark session.
        table (str): The name of the table being processed.
        table_config (dict): Contains the 'load_date_column' used for the snapshot timestamp.
        input (str): Source table prefix.
        output (str): Target unified table name.
    """
    load_column = table_config[table]["load_date_column"]
    if not load_column:
        raise ValueError(
            f"Missing 'load_date_column' for table '{table}' in table_config."
        )

    @dlt.append_flow(
        target=f"{output}", name=f"append_flow_to_union_{table}", once=True
    )
    def create_single_quarantine_table():
        # dlt.read() resolves in-pipeline tables by their bare name (unlike spark.readStream.table(),
        # which needed the "live." prefix), so no prefix branching is needed here.
        df = dlt.read(f"{input}{table}")

        return df.groupBy(f.lit(f"{table}").alias("table")).agg(
            f.count("*").alias("count"),
            f.sum(
                f.when(f.col(f"{DQ_HARD_PREFIX}quarantine") == 1, 1).otherwise(0)
            ).alias("hard_quarantine"),
            f.sum(
                f.when(f.col(f"{DQ_SOFT_PREFIX}quarantine") == 1, 1).otherwise(0)
            ).alias("soft_quarantine"),
            f.max(f.col(load_column)).alias("timestamp"),
        )


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
        input_path = f"{input}{table}" if "." in input else f"live.{input}{table}"
        df = spark.readStream.option("skipChangeCommits", "true").table(input_path)

        unified_df = df.select(
            f.lit(table).alias("source_table"),
            f.col(load_column).alias("ingestion_timestamp"),
            f.to_json(f.struct(f.col("*"))).alias("quarantined_record_json"),
        )
        return unified_df
