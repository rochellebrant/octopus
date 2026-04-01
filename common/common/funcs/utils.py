import re
from typing import Any, Dict, List, Optional, Sequence, Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as f


# ==============================================================================
# 1. GENERAL UTILITY HELPERS (STRINGS & DICTIONARIES)
# ==============================================================================

def ensure_dict_key_prefix(target_dict: Dict[str, Any], prefix: str) -> Dict[str, Any]:
    """
    Returns a new dictionary where all keys are guaranteed to start with the specified prefix.
    Smartly handles partial overlaps to avoid duplicating parts of the prefix.
    
    Args:
        target_dict (Dict[str, Any]): The original dictionary.
        prefix (str): The string prefix to ensure on every key.
        
    Returns:
        Dict[str, Any]: A new dictionary with normalized keys.
        
    Example:
        prefix = "dev_gold_model_"
        keys = ["dev_gold_model_table", "gold_model_table", "table"]
        # Resulting keys will ALL perfectly become "dev_gold_model_table"
    """
    if not prefix:
        return target_dict.copy()
        
    updated_dict = {}
    for key, value in target_dict.items():
        # 1. If it already has the exact prefix, keep it as is
        if key.startswith(prefix):
            updated_dict[key] = value
            continue
            
        # 2. Smart overlap detection: Find the longest end-chunk of the prefix 
        # that matches the start-chunk of the key (e.g., "gold_model_")
        overlap_found = False
        for i in range(len(prefix), 0, -1):
            partial_prefix = prefix[-i:]
            if key.startswith(partial_prefix):
                # Prepend only the missing part of the prefix
                updated_dict[prefix[:-i] + key] = value
                overlap_found = True
                break
                
        # 3. If there is no overlap at all, just safely prepend the whole thing
        if not overlap_found:
            updated_dict[prefix + key] = value
            
    return updated_dict


def remove_all_angle_brackets(s: str) -> str:
    """
    Removes generic type brackets (e.g., <...>) from a string. 
    Often used to clean up complex Spark data types so they render correctly in DBML.
    
    Args:
        s (str): The input string containing angle brackets.
        
    Returns:
        str: The cleaned string with all angle brackets and their contents removed.
    """
    pattern = r"<[^<>]*>"
    while re.search(pattern, s):
        s = re.sub(pattern, "", s)
    return s


# ==============================================================================
# 2. SQL FORMATTING HELPERS
# ==============================================================================

def _sql_lit(v: Any) -> str:
    """
    Safely converts a Python value into a SQL-formatted literal string.
    
    Args:
        v (Any): The Python value to convert (None, int, float, str, etc.).
        
    Returns:
        str: A SQL-safe string representation of the value (e.g., 'NULL', '123', or "'text'").
    """
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def _quote_3part(name: str) -> str:
    """
    Ensures a fully qualified Unity Catalog table name (catalog.schema.table) 
    is properly formatted with backticks around each part.
    
    Args:
        name (str): The raw table name (e.g., my_catalog.my_schema.my_table).
        
    Returns:
        str: A safely backticked 3-part name (e.g., `my_catalog`.`my_schema`.`my_table`).
        
    Raises:
        Exception: If the provided name does not contain exactly 3 parts.
    """
    parts = [p.strip("`") for p in name.split(".")]
    if len(parts) != 3:
        raise Exception(f">>> ⚠️ target must be 3-part: catalog.schema.table, got: {name}")
    return ".".join([f"`{p}`" for p in parts])


# ==============================================================================
# 3. CATALOG & SCHEMA METADATA
# ==============================================================================

def table_exists(spark, full_name: str) -> bool:
    """
    Checks if a table exists in the Databricks environment. 
    Supports both traditional Hive Metastore (1 or 2 part names) and Unity Catalog (3 part names).
    
    Args:
        spark (SparkSession): The active Spark session.
        full_name (str): The table name (e.g., 'table', 'schema.table', or 'catalog.schema.table').
        
    Returns:
        bool: True if the table exists, False otherwise.
    """
    name = full_name.replace("`", "")
    parts = name.split(".")
    if len(parts) == 1:
        return spark.catalog.tableExists(name)
    elif len(parts) == 2:
        db, tbl = parts
        return spark.catalog.tableExists(db, tbl)
    else:
        # For Unity Catalog 3-part names, use information_schema (reliable & explicit)
        cat, sch, tbl = parts
        q = f"""
          SELECT 1
          FROM `{cat}`.information_schema.tables
          WHERE table_schema = '{sch}' AND table_name = '{tbl}'
          LIMIT 1
        """
        return spark.sql(q).count() > 0


def get_table_schema(spark, fq_schema: str, table_name: str) -> DataFrame:
    """
    Extracts column names, data types, and comments from a Databricks table.
    Filters out Delta statistics and partitioning metadata to return a clean schema view.
    
    Args:
        spark (SparkSession): The active Spark session.
        fq_schema (str): The fully qualified catalog and schema name (e.g., 'catalog.schema').
        table_name (str): The name of the table.
        
    Returns:
        DataFrame: A PySpark DataFrame containing 'col_name', 'data_type', and 'comment'.
        
    Raises:
        Exception: If the DESCRIBE TABLE command fails.
    """
    try:
        describe_df = spark.sql(f"DESCRIBE TABLE EXTENDED {fq_schema}.{table_name}")
        # Standardize the output so it only returns actual columns
        schema_df = describe_df.filter(
            f.col("col_name").isNotNull() & (f.col("col_name") != "")
        ).select("col_name", "data_type", "comment")
        return schema_df
    except Exception as e:
        raise Exception(f">>> Error getting table schema for {fq_schema}.{table_name}: {e}")


def generate_table_list(spark, catalog_name: str, schema_name: str, table_type: str = "MANAGED") -> List[str]:
    """
    Retrieves a list of table names from a specific catalog and schema based on the table type.
    
    Args:
        spark (SparkSession): The active Spark session.
        catalog_name (str): The name of the Unity Catalog.
        schema_name (str): The name of the database/schema.
        table_type (str, optional): The type of table to filter by (default is "MANAGED").
        
    Returns:
        List[str]: A list of table names.
    """
    query = f"""
                 SELECT * FROM {catalog_name}.information_schema.tables
                where table_schema = '{schema_name}'
            """
    table_list = spark.sql(query)
    table_list = table_list.filter(f.col("table_type") == table_type).select("table_name")
    list_of_tables = table_list.collect()
    list_of_tables = [x[0] for x in list_of_tables]
    return list_of_tables


def generate_table_key_data(spark, tables: List[str], SCHEMA: str) -> Optional[DataFrame]:
    """
    Extracts Primary Key and Foreign Key constraints for a list of tables using DESCRIBE TABLE EXTENDED.
    
    Args:
        spark (SparkSession): The active Spark session.
        tables (List[str]): A list of table names to inspect.
        SCHEMA (str): The schema/database where the tables reside.
        
    Returns:
        Optional[DataFrame]: A DataFrame containing the parsed constraint metadata 
        (table, key, reference_type, column, reference_table, reference_column), 
        or None if no keys are found.
    """
    final_data = []
    for table in tables:
        # Note: Using table name directly in SQL - ensure SCHEMA and table are sanitized
        references = spark.sql(f"DESCRIBE TABLE EXTENDED {SCHEMA}.{table}")
        references = references.withColumn("table", f.lit(table))
        # Filter for rows that actually contain KEY info
        references = references.filter(f.col("data_type").contains("KEY"))
        final_data.append(references)

    if not final_data:
        return None

    accumulated_results_df = final_data[0]
    for df in final_data[1:]:
        accumulated_results_df = accumulated_results_df.union(df)

    accumulated_results_df = accumulated_results_df.select(
        "table", f.col("data_type").alias("key")
    )

    accumulated_results_df = accumulated_results_df.withColumn(
        "reference_type",
        f.when(f.col("key").contains("PRIMARY"), f.lit("PRIMARY")).otherwise("FOREIGN"),
    )

    accumulated_results_df = accumulated_results_df.withColumn(
        "column", f.regexp_extract(f.col("key"), r"KEY\s+\(`?([^`\s\)]+)`?\)", 1)
    )

    accumulated_results_df = accumulated_results_df.withColumn(
        "reference_table",
        f.when(
            f.col("reference_type") == "FOREIGN",
            f.regexp_extract(f.col("key"), r"REFERENCES\s+.*?\.?.*?\.?`?([^`\s\(\.]+)", 1)
        ).otherwise(f.lit(None)),
    )

    accumulated_results_df = accumulated_results_df.withColumn(
        "reference_column",
        f.when(
            f.col("reference_type") == "FOREIGN",
            f.regexp_extract(f.col("key"), r"\(`?([^`\s\)]+)`?\)$", 1),
        ).otherwise(f.lit(None)),
    )

    return accumulated_results_df


def generate_fk_script_df(accumulated_df: DataFrame, SCHEMA: str) -> DataFrame:
    """
    Transforms the output of `generate_table_key_data` into executable ALTER TABLE scripts 
    for applying Foreign Key constraints.
    
    Args:
        accumulated_df (DataFrame): The DataFrame containing parsed key metadata.
        SCHEMA (str): The schema/database prefix to use in the generated SQL scripts.
        
    Returns:
        DataFrame: A DataFrame containing the generated SQL scripts under the 'script' column.
    """
    # Filter for Foreign Keys
    accumulated_df_fk = accumulated_df.filter(
        f.col("reference_type").contains("FOREIGN")
    ).select(f.col("table"), f.col("key"))
    
    # Extract the column name inside the first set of parentheses
    accumulated_df_fk = accumulated_df_fk.withColumn(
        "fk_col", f.regexp_extract(f.col("key"), r"FOREIGN KEY\s+\(`?([^`\s\)]+)`?\)", 1)
    )

    # Clean up the constraint string
    # Take the raw 'key' string from DESCRIBE TABLE (e.g. "FOREIGN KEY (col) REFERENCES table(id)")
    accumulated_df_fk = accumulated_df_fk.withColumn(
        "constraint", f.col("key")
    )

    # Generate a unique name for the constraint
    accumulated_df_fk = accumulated_df_fk.withColumn(
        "fk_name", f.concat(f.col("table"), f.lit("_"), f.col("fk_col"), f.lit("_fk"))
    )

    # Build the ALTER TABLE script
    accumulated_df_fk = accumulated_df_fk.withColumn(
        "script",
        f.concat(
            f.lit(f"ALTER TABLE {SCHEMA}."),
            f.col("table"),
            f.lit(" ADD CONSTRAINT "),
            f.col("fk_name"),
            f.lit(" "),
            f.col("constraint"),
        ),
    )
    return accumulated_df_fk


# ==============================================================================
# 5. DELTA WRITE & MERGE OPERATIONS
# ==============================================================================

def overwrite_to_delta(spark, df: DataFrame, target: str) -> None:
    """
    Overwrites a DataFrame to a Delta table in Databricks. 
    Automatically enables schema overwriting to handle schema evolution.
    
    Args:
        spark (SparkSession): The active Spark session.
        df (DataFrame): The input PySpark DataFrame.
        target (str): The full target table name/path.
    """
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    print(f">>> Data overwritten to {target}")


def append_to_delta(spark, df: DataFrame, target: str) -> None:
    """
    Appends a DataFrame to an existing Delta table in Databricks.
    Automatically merges the schema to accommodate new columns.
    
    Args:
        spark (SparkSession): The active Spark session.
        df (DataFrame): The input PySpark DataFrame.
        target (str): The full target table name/path.
    """
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target)
    print(f">>> Data appended to {target}")


def unify_tables(spark, union_mapping: dict) -> None:
    """
    Unifies multiple source tables into a single target table per mapping.

    Args:
        spark (SparkSession): The active Spark session.
        union_mapping (dict): A dictionary where each key is the name of a target
                              table and its value is a list of source table names
                              to be unified (unioned) into that target.
    
    Process:
        - Loads the first source table into a Spark DataFrame.
        - Iteratively unions all remaining source tables using `unionByName`,
          which allows for schema mismatches by matching columns by name.
        - Overwrites the existing target table (if it exists) with the unified
          DataFrame in Delta format.
        - Logs the completion of each target table save.

    Notes:
        - This function drops and recreates the target table via overwrite.
        - Suitable for combining datasets with compatible but possibly differing
          schemas (e.g., optional or evolving columns).
    """
    for target_table, source_tables in union_mapping.items():
        unified_df = spark.table(source_tables[0])
        
        for table in source_tables[1:]:
            df = spark.table(table)
            unified_df = unified_df.unionByName(df, allowMissingColumns=True)  # Use unionByName to handle schema differences
        
        unified_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        print(f">>> Unified data saved to: {target_table}")


def merge_into_delta(spark,
                     df: DataFrame,
                     target: str,
                     match_keys: list,
                     delete_mode: str = "none",
                     period_column: Optional[str] = None,
                     scope_values: Optional[Sequence[Union[str, int, float]]] = None,
                     delete_filter: Optional[str] = None,
                     include_period_in_keys: bool = True
                     ) -> None:
    """
    A robust generic Delta MERGE operation capable of handling schema evolution,
    updates, inserts, and optional scoped/full deletion.

    Args:
        spark (SparkSession): The active Spark session.
        df (DataFrame): The incoming PySpark DataFrame to merge into the target.
        target (str): The fully qualified 3-part Unity Catalog target table name.
        match_keys (list): A list of column names representing the unique composite key for the merge.
        delete_mode (str): 
            - "none" (default): No deletions occur.
            - "scoped": Deletes target rows missing from the source, but ONLY within the specific periods found in the batch.
            - "full": Deletes target rows globally if they are missing from the source (can be constrained by delete_filter).
        period_column (str, optional): The column representing the timeframe (required if delete_mode is 'scoped').
        scope_values (list, optional): An explicit list of period values to limit deletes to. If missing, it's computed from the batch.
        delete_filter (str, optional): An extra SQL predicate to limit full/scoped deletes (e.g., "tgt.partition_date >= '2025-07-01'").
        include_period_in_keys (bool): If True and delete_mode is 'scoped', the period_column is automatically added to the match_keys.

    Raises:
        Exception: If constraints are violated (e.g., missing keys, invalid table formatting) or merge fails.
    """
    try:
        # Prep ---------------------------------------------------------
        target = _quote_3part(target)
        if not match_keys:
            raise Exception(">>> ⚠️ match_keys must be a non-empty list.")
        missing_keys = [k for k in match_keys if k not in df.columns]
        if missing_keys:
            raise Exception(f">>> ⚠️ match_keys missing from DataFrame: {missing_keys}")

        # If we're doing scoped deletes, we need a period column
        if delete_mode == "scoped":
            if not period_column:
                raise Exception(">>> ⚠️ period_column is required when delete_mode='scoped'.")
            if period_column not in df.columns:
                raise Exception(f">>> ⚠️ period_column '{period_column}' not found in DataFrame.")

        # Optionally include period in match keys
        if delete_mode == "scoped" and include_period_in_keys and period_column not in match_keys:
            match_keys = list(match_keys) + [period_column]

        # Table exists ---------------------------------------------------------
        if not table_exists(spark, target):
            print(f">>> Creating {target} (table not found).")
            # Use SQL so your backticked 3-part name from _quote_3part(target) works
            view_name = "source_table_temp"
            df.createOrReplaceTempView(view_name)
            spark.sql(f"CREATE TABLE IF NOT EXISTS {target} USING DELTA AS SELECT * FROM `{view_name}` WHERE 1=0")


        # Schema alignment ---------------------------------------------------------
        existing_cols = set(spark.table(target).columns)
        incoming_cols = set(df.columns)

        # Pad DF with missing target columns
        pad_cols = [c for c in existing_cols if c not in incoming_cols]
        if pad_cols:
            print(f">>> ⚠️ Padding missing source columns with NULLs: {pad_cols}")
            for c in pad_cols:
                df = df.withColumn(c, f.lit(None))

        # Evolve table with new DF columns (as STRING by default)
        new_cols = [c for c in incoming_cols if c not in existing_cols]
        if new_cols:
            print(f">>> New source columns detected (adding as STRING): {new_cols}")
            add_cols = ", ".join(f"`{c}` {df.schema[c].dataType.simpleString()}" for c in new_cols)
            spark.sql(f"ALTER TABLE {target} ADD COLUMNS ({add_cols})")
        
        # Recompute incoming_cols after padding (order = original DF + padded at end)
        incoming_cols = list(df.columns)

        print(f">>> Match keys: {match_keys}")

        # Temp view ---------------------------------------------------------------
        view_name = "source_table_temp"
        df.createOrReplaceTempView(view_name)

        # Build match condition (null-safe)
        match_cond = " AND ".join([f"(tgt.`{k}` <=> src.`{k}`)" for k in match_keys])

        # print(f">>> Match conditions: {match_conditions}")
        
        # Update clause -----------------------------------------------------------
        # Update only when any non-key column differs
        update_cols = [c for c in incoming_cols if c not in match_keys]
        if update_cols:
            upd_cond = " OR ".join([f"NOT (tgt.`{c}` <=> src.`{c}`)" for c in update_cols])
            upd_set  = ", ".join([f"tgt.`{c}` = src.`{c}`" for c in update_cols])
            update_sql = f"""
            WHEN MATCHED AND ({upd_cond}) THEN
            UPDATE SET {upd_set}
            """
        else:
            update_sql = ""

        # ------------------------ PRE-MERGE SCOPED/FULL DELETE ------------------------
        if delete_mode in ("scoped", "full"):
            if delete_mode == "scoped":
                if scope_values is None:
                    scope_values = [r[0] for r in df.select(period_column).distinct().collect()]
                if not scope_values:
                    raise Exception(f">>> ⚠️ No values for period_column '{period_column}' found in batch.")
                in_list = ", ".join(_sql_lit(v) for v in scope_values)
                scope_pred = f"tgt.`{period_column}` IN ({in_list})"
                if delete_filter:
                    scope_pred = f"({scope_pred}) AND ({delete_filter})"
            else:  # full
                scope_pred = delete_filter if delete_filter else "1=1"

            # DELETE rows in scope that no longer exist in the source
            # use the SAME match_cond you build later
            # (we can build it here too, since match_keys is already finalized)
            predelete_match_cond = " AND ".join([f"(tgt.`{k}` <=> src.`{k}`)" for k in match_keys])

            predelete_sql = f"""
            DELETE FROM {target} AS tgt
            WHERE ({scope_pred})
            AND NOT EXISTS (
                SELECT 1
                FROM `{view_name}` AS src
                WHERE {predelete_match_cond}
            )
            """
            spark.sql(predelete_sql)


        # Insert clause -----------------------------------------------------------
        insert_cols = ", ".join([f"`{c}`" for c in incoming_cols])
        insert_vals = ", ".join([f"src.`{c}`" for c in incoming_cols])

        sql = f"""
        MERGE INTO {target} AS tgt
        USING `{view_name}` AS src
        ON ({match_cond})
        {update_sql}
        WHEN NOT MATCHED THEN
        INSERT ({insert_cols})
        VALUES ({insert_vals})
        """
        spark.sql(sql)
    except Exception as e:
        raise Exception(f">>> Error in merge_into_delta function: {e}")