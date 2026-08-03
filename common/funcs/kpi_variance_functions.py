from pyspark.sql import Window, functions as f, DataFrame
from typing import List, Dict, Optional


def filter_for_active_records(
    spark, df: DataFrame, group_cols: List[str], timestamp_col: str
):
    """
    Filters DataFrame to include only active records where the specified column is NULL.
    """
    window_specification = Window.partitionBy(*group_cols).orderBy(
        f.col(timestamp_col).desc()
    )
    df_with_row_num = df.withColumn(
        "row_num", f.row_number().over(window_specification)
    )
    latest_records_df = df_with_row_num.filter(f.col("row_num") == 1).drop("row_num")

    return latest_records_df


def rank_data(
    spark, df: DataFrame, partition_by: Optional[List[str]], order_by: Dict[str, str]
):
    """
    Ranks a DataFrame based on the specified partition and order columns.
    """
    if partition_by:
        window_specification = Window.partitionBy(*partition_by)
    else:
        window_specification = Window

    order_expr = []
    for column, direction in order_by.items():
        if direction == "asc":
            order_expr.append(f.col(column).asc())
        elif direction == "desc":
            order_expr.append(f.col(column).desc())
        else:
            raise ValueError(
                f"Invalid sorting direction: {direction}. Must be 'asc' or 'desc'."
            )

    window_specification = window_specification.orderBy(*order_expr)
    return df.withColumn("rank", f.row_number().over(window_specification))


def get_columns_with_prefixes(spark, df, prefixes):
    """
    Retrieves columns from a DataFrame that start with any of the specified prefixes.
    """
    return [
        col for col in df.columns if any(col.startswith(prefix) for prefix in prefixes)
    ]


def get_absolute_column_expressions(spark, columns):
    """
    Generates absolute value expressions for a list of column names.
    """
    return [f.abs(f.col(col_name)) for col_name in columns]


def get_columns_by_datatypes(spark, df, datatypes):
    """
    Returns a list of columns in the DataFrame that match the specified datatypes.
    """
    cols = []
    for col, dtype in df.dtypes:
        if dtype in datatypes:
            cols.append(col)
    return cols


def compute_rolling_stats_and_flags(
    spark,
    df,
    partition_by: List[str],
    numeric_columns: List[str],
    rolling_window_length=3,
    pct_diff_threshold=10,  # 10%
    match_keys=[],
    drop_other_columns=False,
    drop_numeric_columns=False,
    drop_rolling_calculations=False,
    flag_high_pct_variance=False,
    flag_null_values=False,
):
    """
    Processes specified columns in the DataFrame:
    - Computes moving averages.
    - Calculates percentage differences with optional flags for exceeding thresholds or null values.
    """
    percentage_diff_columns = []
    raw_columns = df.columns
    pct_diff_col_prefix = "pct_variance_"
    moving_avg_col_prefix = "rolling_avg_"

    rolling_avg_window = (
        Window.partitionBy(*partition_by)
        .orderBy("rank")
        .rowsBetween(1, rolling_window_length)
    )

    for col_name in numeric_columns:
        moving_avg_col_name = f"{moving_avg_col_prefix}{col_name}"
        percentage_diff_col_name = f"{pct_diff_col_prefix}{col_name}"

        # Moving average
        df = df.withColumn(
            moving_avg_col_name, f.avg(col_name).over(rolling_avg_window)
        )

        # Percentage difference calculation
        df = df.withColumn(
            percentage_diff_col_name,
            f.when(
                (f.col(col_name).isNotNull() & f.col(moving_avg_col_name).isNotNull()),
                # ((f.col(col_name) - f.col(moving_avg_col_name)) / f.col(moving_avg_col_name)) * 100
                f.try_divide(
                    (f.col(col_name) - f.col(moving_avg_col_name)) * 100,
                    f.col(moving_avg_col_name),
                ),
            ).otherwise(None),
        )
        percentage_diff_columns.append(percentage_diff_col_name)

        # Flag high variance
        if pct_diff_threshold is not None and flag_high_pct_variance:
            variance_flag_name = f"flag_{pct_diff_col_prefix}{col_name}"
            df = df.withColumn(
                variance_flag_name,
                f.when(
                    (
                        f.col(col_name).isNotNull()
                        & f.col(moving_avg_col_name).isNotNull()
                    )
                    &
                    # (f.abs((f.col(col_name) - f.col(moving_avg_col_name)) / f.col(moving_avg_col_name)) * 100 > pct_diff_threshold),
                    (
                        f.expr(
                            f"f.try_divide(f.abs(({col_name} - {moving_avg_col_name}) * 100), {moving_avg_col_name})"
                        )
                        > pct_diff_threshold
                    ),
                    f.lit("1"),
                ).otherwise(None),
            )

    # Calculate max % difference
    df = df.withColumn(
        "max_pct_diff",
        f.greatest(*[f.abs(f.col(col)) for col in percentage_diff_columns]),
    )

    # Identify column with max variance
    case_expr = "CASE "
    for col_name in percentage_diff_columns:
        case_expr += f"WHEN abs(max_pct_diff) = abs({col_name}) THEN '{col_name[len(pct_diff_col_prefix):]}' "
    case_expr += "END"

    df = df.withColumn("max_variance_column", f.expr(case_expr))

    # Drop unnecessary columns based on user-defined flags
    if drop_other_columns:
        other_columns = list(
            set(raw_columns) - set(match_keys) - set(numeric_columns) - {"rank"}
        )
        df = df.drop(*other_columns)

    if drop_numeric_columns:
        df = df.drop(*numeric_columns)

    if drop_rolling_calculations:
        col_prefixes_to_drop = [moving_avg_col_prefix, pct_diff_col_prefix, "rank"]
        drop_columns = get_columns_with_prefixes(spark, df, col_prefixes_to_drop)
        df = df.drop(*drop_columns)

    return df.orderBy(*partition_by, "rank")


def calculate_kpi_variances(
    spark, target_database: str, prefix: str, kpi_table: str, models_config
):
    data_types = ("double", "float")
    temp_table = "temp_kpi_variance"
    table_prefix = f"{target_database}.{prefix}"
    final_table = f"{table_prefix}{kpi_table}"

    for table in (
        t for t in models_config if "kpi_variance" in models_config.get(t, {})
    ):
        kpi_config = models_config[table]["kpi_variance"]

        partition_by = kpi_config["partition_by"]
        order_by = kpi_config["order_by"]
        rolling_window_length = kpi_config["rolling_window_length"]
        exclusion_columns = kpi_config.get("exclusion_columns", [])
        source_table = f"{table_prefix}{table}"

        src_data = spark.table(source_table)

        match_keys = models_config[table]["match_keys"]
        timestamp_column = models_config[table]["load_date_column"]
        numeric_cols_all = get_columns_by_datatypes(spark, src_data, data_types)
        numeric_cols_to_process = [
            item for item in numeric_cols_all if item not in exclusion_columns
        ]

        filtered_active_data = filter_for_active_records(
            spark, src_data, match_keys, timestamp_column
        )

        ranked_active_data = rank_data(
            spark, filtered_active_data, partition_by=partition_by, order_by=order_by
        )

        df = compute_rolling_stats_and_flags(
            spark,
            ranked_active_data,
            partition_by,
            numeric_columns=numeric_cols_to_process,
            rolling_window_length=rolling_window_length,
            pct_diff_threshold=None,
            match_keys=match_keys,
            drop_other_columns=True,
            drop_numeric_columns=True,
            drop_rolling_calculations=True,
            flag_high_pct_variance=False,
            flag_null_values=False,
        )

        views_df = spark.sql(f"SHOW VIEWS IN {target_database}").toPandas()
        existing_views = set(views_df["viewName"])

        if temp_table in existing_views:
            existing_data = spark.sql(f"SELECT * FROM {temp_table}")
            updated_data = existing_data.union(df)
        else:
            updated_data = df

        updated_data.createOrReplaceTempView(temp_table)
        print(f">>> Processed {prefix}{table}")

    spark.sql(f"SELECT * FROM {temp_table}").write.mode("overwrite").saveAsTable(
        f"{final_table}"
    )
    print(f">>> Saved to {final_table}")
