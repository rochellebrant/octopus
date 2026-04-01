from pyspark.sql import DataFrame, functions as f
from datetime import datetime
import os
import sys
import importlib

def import_sql_queries(notebook_dir, sql_query_folder="sql_queries"):
    """
    Dynamically loads SQL queries from a given subfolder.
    Returns a dictionary mapping module names to their SQL string values.
    """
    sql_queries_dict = {}

    parent_dir = os.path.dirname(notebook_dir)
    subfolder_path = os.path.join(parent_dir, sql_query_folder)

    if not os.path.exists(subfolder_path):
        print(f">>> Directory {subfolder_path} does not exist.")
        return sql_queries_dict

    print(f">>> Importing sql_queries from {subfolder_path}")
    sys.path.append(subfolder_path)

    for file in os.listdir(subfolder_path):
        if file.endswith(".py"):
            module_name = file[:-3]
            module_path = os.path.join(subfolder_path, file)

            try:
                spec = importlib.util.spec_from_file_location(module_name, module_path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)

                for var_name, var_value in vars(module).items():
                    if isinstance(var_value, str) and not var_name.startswith("__"):
                        sql_queries_dict[module_name] = var_value
                        print(f"\t> Imported {module_name}")

            except Exception as e:
                print(f"\t❌ Failed to import {module_name}: {str(e)}")

    return sql_queries_dict


def validate_is_dict(name: str, var: any) -> None:
    if not isinstance(var, dict):
        raise ValueError(f'⚠️ Missing or invalid `{name}`. Expected a dictionary. Received: {type(var)}.')


def validate_is_list(name: str, var: any) -> None:
    if not isinstance(var, list) or len(var) == 0:
        raise ValueError(f"⚠️ Missing or invalid `{name}`. Expected a non-empty list. Received: {type(var)}.")


def validate_is_string(name: str, var: any) -> None:
    if not isinstance(var, str):
        raise ValueError(f"⚠️ Missing or invalid `{name}`. Expected a string. Received: {type(var)}.")


def get_cdm_and_data_input_methods(check_config: dict, check_name: str):
    valid_input_methods = {"table", "sql_query"}
    input_method_config = check_config.get("input_method")
    validate_is_dict(f"{check_name}.input_method", input_method_config)

    data_input_method = input_method_config.get("Data")
    cdm_input_method = input_method_config.get("CDM")
    if data_input_method not in valid_input_methods or cdm_input_method not in valid_input_methods:
        raise ValueError(f'⚠️ Invalid `input_method.Data` or `input_method.CDM` for check {check_name}.\
                            Expected one of: ["table, "sql_query"].\
                            Received: {data_input_method} and {cdm_input_method}.')
    return (data_input_method, cdm_input_method)


def get_join_type(check_config: dict, check_name: str) -> str:
    valid_join_types = {"left", "fullouter"}
    join_type = check_config.get("join_type", "fullouter").lower()

    if join_type not in valid_join_types:
        raise ValueError(f"⚠️ Invalid `join_type` for check `{check_name}`. "
                         f"Expected one of: {valid_join_types}. Received: {join_type}")
    return join_type


def validate_join_columns_config(join_columns: list, check_name: str) -> None:
    validate_is_dict(f"{check_name}.join_columns", join_columns)
    primary_config = join_columns.get("primary", {})
    if not primary_config:
        raise ValueError(f'⚠️ Expected "primary" key in `join_columns` for check {check_name}. Received: {primary_config.keys()}')
    validate_is_list("join_columns.primary", primary_config)
    primary_dicts = primary_config[0]
    validate_is_dict(f"{check_name}.join_columns.primary[0]", primary_dicts)
    k = primary_dicts.keys()
    if "CDM" not in k or "Data" not in k:
        raise ValueError(f'⚠️ Expected "CDM" and "Data" keys in `join_columns.primary` for check {check_name}. Received: {k}')

def get_join_columns_config(check_config: dict, check_name: str):
    join_columns = check_config.get("join_columns")
    validate_join_columns_config(join_columns, check_name)
    return join_columns

def validate_table_presence(table_name: str, tables_config: dict, check_name: str) -> None:
    table = tables_config.get(table_name)
    if not table:
        raise ValueError(f"⚠️ Missing `tables.{table_name}` for check {check_name}. Expected a table in the format `catalog.database.table`. Received {table}.")
    
def validate_sql_queries_config(sql_queries_dict: dict, check_name: str, source_key: str):
    validate_is_dict(f"{check_name}.input_method.sql_queries", sql_queries_dict)
    validate_is_string(f"{check_name}.input_method.sql_queries.{source_key}", sql_queries_dict[source_key])


def get_full_name_space_target_table(check_name: str, prefix: str = None, database: str = None):
    return f"{database}.{prefix}__cdm_checker__{check_name}"


def fetch_table_or_query(spark,
                         fetch_method: str,
                         check_config,
                         check_name: str,
                         source_key: str,
                         sql_queries_dict,
                         raw_tbl_prefix: str = None,
                         bronze_tbl_prefix: str = None,
                         silver_tbl_prefix: str = None,
                         gold_tbl_prefix: str = None
                         ):
    """
    Fetch a Spark DataFrame using either a table name or a SQL query, based on the fetch method.

    Parameters:
        spark (SparkSession): The active Spark session.
        fetch_method (str): Either 'table' or 'sql_query'.
        check_config (dict): Configuration dictionary containing table or SQL query details.
        check_name (str): The name of the check (used for logging and error messages).
        source_key (str): The key to fetch ('Data' or 'CDM').

    Returns:
        DataFrame: The resulting Spark DataFrame.
    """
    if fetch_method == "table":
        table_config = check_config.get("tables", {})
        validate_table_presence(source_key, table_config, check_name)
        return spark.table(table_config[source_key])

    elif fetch_method == "sql_query":
        query_config = check_config.get("sql_queries", {})
        validate_sql_queries_config(query_config, check_name, source_key)
        query_name = query_config.get(source_key)
        if query_name.endswith(".py"):
            query_name = query_name[:-3]
            
        query = sql_queries_dict.get(query_name)
        query = query.format(
            raw_prefix=raw_tbl_prefix,
            bronze_prefix=bronze_tbl_prefix,
            silver_prefix=silver_tbl_prefix,
            gold_prefix=gold_tbl_prefix,
        )
        if query:
            return spark.sql(query)
        else:
            raise ValueError(
                f"⚠️ No SQL query called `{query_name}` was imported. "
                f"Ensure it exists and matches the name in the config file."
            )

    else:
        raise ValueError(f"Invalid fetch method: {fetch_method}. Use 'table' or 'sql_query'.")

def ensure_required_columns(spark, check_name, required_cols, df, source_key: str):
    """
    Validate that required columns exist in the fetched SQL table and DataFrame.
    Raises an error if columns are missing.
    """
    missing_cols = required_cols - set(df.columns)

    if missing_cols:
        raise ValueError(f"❌ Mismatched columns in check `{check_name}`: "
                         f"Missing in `{source_key}` df: {missing_cols}")

def get_column_join_mapping_list(spark, join_columns_config):
    primary_mapping_list = join_columns_config.get("primary", [])
    secondary_mapping_list = join_columns_config.get("secondary", [])
    return primary_mapping_list, secondary_mapping_list

def get_column_join_mapping_dict(spark, primary_col_mapping_list: list, secondary_col_mapping_list: list) -> tuple[dict, dict]:
    primary_col_map = {col["Data"]: col["CDM"] for col in primary_col_mapping_list}
    secondary_col_map = {col["Data"]: col["CDM"] for col in secondary_col_mapping_list}
    return primary_col_map, secondary_col_map

def validate_summary_table_config(config):
    summary_table_config = config.get("summary_table").get("save_to")
    validate_is_dict("summary_table.save_to", summary_table_config)
    if "catalog" not in summary_table_config or "database" not in summary_table_config:
        raise ValueError(f'>>> Expected "catalog" and "database" keys in `summary_table.save_to`. Recieved: {summary_table_config.keys()}')
    validate_is_string("summary_table.save_to", summary_table_config["catalog"])
    validate_is_string("summary_table.save_to", summary_table_config["database"])

def get_summary_table(config):
    validate_summary_table_config(config)
    summary_catalog = config.get("summary_table").get("save_to").get("catalog")
    summary_database = config.get("summary_table").get("save_to").get("database")
    summary_table = f"{summary_catalog}.{summary_database}.cdm_checker_summary"
    return summary_table

def get_required_columns(spark, primary_col_mapping_list: list, secondary_col_mapping_list: list) -> list:
    cdm_columns = [col["CDM"] for col in primary_col_mapping_list]
    cdm_columns += [col["CDM"] for col in secondary_col_mapping_list]
    data_columns = [col["Data"] for col in primary_col_mapping_list]
    data_columns += [col["Data"] for col in secondary_col_mapping_list]
    return cdm_columns, data_columns

def add_column_prefix(spark, df: DataFrame, prefix: str) -> DataFrame:
    """
    Renames all columns in the given DataFrame by adding a specified prefix.
    """
    return df.select(*[df[col].alias(f"{prefix}{col}") for col in df.columns])

def build_join_conditions(df_left, df_right, column_map, right_prefix=""):
    """
    Builds join conditions for joining two DataFrames.

    :param df_left: The left DataFrame.
    :param df_right: The right DataFrame.
    :param column_map: Dictionary mapping df_left columns to df_right columns.
    :param right_prefix: Optional prefix for column names in df_right.
    :return: List of join conditions.
    """
    return [
        df_left[left_col] == df_right[f"{right_prefix}{right_col}"]
        for left_col, right_col in column_map.items()
    ]

def normalise_whitespace(df, input_col, output_col):
    return df.withColumn(output_col, f.regexp_replace(f.col(input_col), "\\s+", " "))

def lowercase_and_trim(df, input_col, output_col):
    return df.withColumn(output_col, f.trim(f.lower(f.col(input_col))))

def replace_limited_with_ltd(df, input_col, output_col):
    return df.withColumn(output_col, f.regexp_replace(f.col(input_col), "limited", "ltd"))

def remove_punctuation(df, input_col, output_col):
    return df.withColumn(output_col, f.regexp_replace(f.col(input_col), "[,\\.]", ""))


def join_dataframes(spark, df_left, df_right, column_map, right_prefix: str = "", join_type: str = "fullouter"):
    """
    Joins two DataFrames based on generated join conditions. The join defaults to full outer join it not specified.
    """
    join_conditions = build_join_conditions(df_left, df_right, column_map, right_prefix)
    return df_left.join(df_right, on=join_conditions, how=join_type)


def add_match_column(spark, df, column_map, match_col="match_found", success_value="Match", fail_value="No Match", right_prefix=""):
    """
    Adds a `match_col` column indicating whether a match is found based on column mappings.
    """
    join_expr = " AND ".join([
        f"{left_col} IS NOT NULL AND {right_prefix}{right_col} IS NOT NULL" 
        for left_col, right_col in column_map.items()
    ])

    return df.withColumn(
        match_col, 
        f.when(f.expr(join_expr), f.lit(success_value)).otherwise(f.lit(fail_value))
    )


def build_case_statement(data_col, cdm_col, cdm_col_prefix="", column_type="primary"):
    """
    Generates a SQL CASE statement for comparing columns from two datasets.
    
    :param data_col: The column name from the data DataFrame.
    :param cdm_col: The column name from the CDM DataFrame.
    :param cdm_col_prefix: The prefix for the column names in the CDM DataFrame (optional).
    :param column_type: Specify whether the columns are "primary" or "secondary" for generating the appropriate mismatch message.
    :return: A SQL CASE statement as a string.
    """
    data_value_col = f"{data_col}"
    cdm_value_col = f"{cdm_col_prefix}{cdm_col}"

    if column_type == "primary":
        mismatch_message = f"mismatch_message_{data_col}_{cdm_col}"
    else:
        mismatch_message = f"other_message_{data_col}_{cdm_col}"

    return f"""
    CASE 
        WHEN {data_value_col} IS NULL AND {cdm_value_col} IS NULL THEN '⚠️ Both Data and CDM values are `NULL`'
        WHEN {data_value_col} IS NULL THEN '❌ CDM {cdm_col} \"' || {cdm_value_col} || '\" is not a {data_col} in the Data'
        WHEN {cdm_value_col} IS NULL THEN '❌ Data {data_col} \"' || {data_value_col} || '\" is not a {cdm_col} in the CDM'
        WHEN {data_value_col} = {cdm_value_col} THEN '✅ Matched \"' || {cdm_value_col} || '\"'
        ELSE '❌ Data {data_col} \"' || {data_value_col} || '\" does not match CDM {cdm_col} \"' || {cdm_value_col} || '\"'
    END AS {mismatch_message}
    """

def add_primary_mismatch_messages_to_df(df, primary_col_map):
    mismatch_columns = [
        f"mismatch_message_{data_col}_{cdm_col}" 
        for data_col, cdm_col in primary_col_map.items()
    ]
    
    df = df.withColumn(
        "join_message", f.concat_ws(",\n", *[f.col(col) for col in mismatch_columns])
    ).drop(*mismatch_columns)
    
    return df

def add_secondary_messages_to_df(df, secondary_col_map, match_success_str="Match"):
    secondary_columns = [
        f"other_message_{data_col}_{cdm_col}"
        for data_col, cdm_col in secondary_col_map.items()
        ]
    
    match_found_condition = f.col("match_found") == match_success_str
    
    df = df.withColumn(
        "secondary_join_message", f.when(match_found_condition, f.concat_ws(",\n", *[f.col(col) for col in secondary_columns]))
        .otherwise(f.lit(None))
    ).drop(*secondary_columns)
    
    return df

def process_primary_conditions(spark, df, primary_column_mapping_dict, cdm_prefix):
    # Generate mismatch conditions
    primary_conditions = [
        build_case_statement(data_col, cdm_col, cdm_prefix, column_type="primary")
        for data_col, cdm_col in primary_column_mapping_dict.items()
    ]

    # Apply conditions to DataFrame
    for condition in primary_conditions:
        df = df.selectExpr("*", condition)

    # Add messages to DataFrame
    df = add_primary_mismatch_messages_to_df(df, primary_column_mapping_dict)

    return df

def process_secondary_conditions(spark, df, secondary_column_mapping_dict, cdm_prefix, match_success_str="Match"):
    # Generate mismatch conditions
    secondary_conditions = [
        build_case_statement(data_col, cdm_col, cdm_prefix, column_type="secondary")
        for data_col, cdm_col in secondary_column_mapping_dict.items()
    ]

    # Apply conditions to DataFrame
    df = df.selectExpr("*", *secondary_conditions)

    # Add messages to DataFrame
    df = add_secondary_messages_to_df(df, secondary_column_mapping_dict, match_success_str)

    return df

def add_column_mapping_to_df(spark, df, column_mapping, column_type="primary"):
    """
    Adds the column mapping to the result DataFrame.

    :param df: The DataFrame to which the column mapping will be added.
    :param column_mapping: Dictionary mapping data columns to CDM columns.
    """
    if column_type == "primary":
        col_name = "column_mapping___data_CDM"
    elif column_type == "secondary":
        col_name = "secondary_column_mapping___data_CDM"
    else:
        raise ValueError("⚠️ Invalid column type provided.")
    column_mapping = ',\n'.join([f'{left_col} ↔ {right_col}' for left_col, right_col in column_mapping.items()])
    df = df.withColumn(col_name, f.lit(column_mapping))
    
    return df


def get_result_df_with_columns(spark, result_df, check_config):
    """
    Adds metadata columns to the result DataFrame based on the input methods ('table' or 'sql_query')
    and their corresponding sources from the check configuration.

    Columns added:
        - cdm_input_method: 'table' or 'sql_query' for CDM data
        - cdm_source: Table name or SQL script filename for CDM data
        - data_input_method: 'table' or 'sql_query' for Data
        - data_source: Table name or SQL script filename for Data
        - cdm_checker_timestamp: Current timestamp when the function is executed

    :param spark: SparkSession object (not used inside but kept for compatibility).
    :param result_df: The input DataFrame to enrich with additional columns.
    :param check_config: Dictionary containing input methods, table names, and SQL queries.
    :return: The updated DataFrame with new metadata columns added.
    """
    def get_source(config, key):
        method = config.get('input_method', {}).get(key)
        if method == 'table':
            source = config.get('tables', {}).get(key)
        elif method == 'sql_query':
            source = config.get('sql_queries', {}).get(key)
            if source and not source.endswith('.py'):
                source += '.py'
        else:
            source = None
        return method, source

    cdm_method, cdm_source = get_source(check_config, 'CDM')
    data_method, data_source = get_source(check_config, 'Data')
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return result_df.selectExpr(
        f"'{cdm_method}' AS cdm_input_method",
        f"'{cdm_source}' AS cdm_source",
        f"'{data_method}' AS data_input_method",
        f"'{data_source}' AS data_source",
        f"'{ts}' AS cdm_checker_timestamp",
        "*"
    )



def reorder_columns(spark, df, column_order, optional_columns):
    """
    Reorders the columns in the DataFrame based on a specified column order and optional columns.
    
    :param df: The DataFrame to reorder.
    :param column_order: List of priority column names in the desired order.
    :param optional_columns: List of optional columns to include, if they exist in the DataFrame.
    :return: The DataFrame with columns reordered.
    """
    existing_optional_columns = [col_name for col_name in optional_columns if col_name in df.columns]
    remaining_columns = [col_name for col_name in df.columns if col_name not in column_order + existing_optional_columns]
    final_column_order = column_order + existing_optional_columns + remaining_columns
    df = df.select([f.col(c) for c in final_column_order])
    
    return df

def count_missing_values(spark, df, column_mapping, right_prefix):
    """
    Counts the number of missing values in the left and right columns from the column mapping.

    :param df: The DataFrame to check for missing values.
    :param column_mapping: Dictionary mapping left columns to right columns.
    :param right_prefix: Prefix used for right columns.
    :return: Tuple containing (missing_in_left_count, missing_in_right_count)
    """
    missing_in_right_expr = ' OR '.join([f"{right_prefix}{right_col} IS NULL AND {left_col} IS NOT NULL" for left_col, right_col in column_mapping.items()])
    missing_in_left_expr = ' OR '.join([f"{left_col} IS NULL AND {right_prefix}{right_col} IS NOT NULL" for left_col, right_col in column_mapping.items()])
    both_missing_expr = ' OR '.join([f"{left_col} IS NULL AND {right_prefix}{right_col} IS NULL" for left_col, right_col in column_mapping.items()])

    # Count missing values
    missing_in_right_count = df.filter(f.expr(missing_in_right_expr)).count() if missing_in_right_expr else 0
    missing_in_left_count = df.filter(f.expr(missing_in_left_expr)).count() if missing_in_left_expr else 0
    both_missing_count = df.filter(f.expr(both_missing_expr)).count() if both_missing_expr else 0

    return missing_in_left_count, missing_in_right_count, both_missing_count


def convert_timestamps(spark, df, timestamp_formats):
    """
    Converts timestamp columns in df to match the format "yyyy-MM-dd HH:mm:ss".
    """
    if not timestamp_formats:
        return df
    for col, fmt in timestamp_formats.items():
        if col in df.columns:
            df = df.withColumn(
                col,
                f.date_format(f.to_timestamp(df[col], fmt), "yyyy-MM-dd HH:mm:ss")
            )
    return df


def convert_dates(df, date_formats):
    """
    Converts date columns in df to match the format "yyyy-MM-dd".
    """
    for col, format_str in date_formats.items():
        if col in df.columns:
            df = df.withColumn(
                col, f.to_date(df[col], format_str, "yyyy-MM-dd")
            )
    return df