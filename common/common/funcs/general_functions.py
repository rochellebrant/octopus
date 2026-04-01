# These functions are used in development, and simply load data into tables in bulk - they are not part of any pipeline and so are not being unit tested. Unit tests can be written when time allows.

from pyspark.sql import functions as f, Window as w

def init_empty_tables(spark, table_config: dict, tables: list, table_prefix: str):
    for table in tables:
        try:
            sql_code_drop = f"""
                        DROP TABLE IF EXISTS {table_prefix}{table}
                        """
            df = spark.sql(sql_code_drop)

            sql_code_create = f"""
                        CREATE TABLE {table_prefix}{table}
                        LIKE {table_config[table]["catalogue_schema"]}.{table}
                        USING DELTA
                    """
            df = spark.sql(sql_code_create)
            print(f">>> Created table: {table_prefix}{table}")

            sql_code_grant = f"""
            GRANT MANAGE ON {table_prefix}{table} TO `Sennen XIO Project` 
            """
            f = spark.sql(sql_code_grant)
        except Exception as e:
            print(f">>> Failed to create table: {table_prefix}{table}\n{e}")


def data_loader(
    spark,
    table_config: dict,
    tables: list,
    table_prefix: str,
    filter_date: str,
    mode: str = "single_day",
):
    if mode not in ["single_day", "multi_day"]:
        print("mode must be either 'single_day' or 'multi_day'")
        return
    if mode == "single_day":
        operator = "="
    else:
        operator = ">="

    for table in tables:
        join_condition = " AND ".join(
            [f"tgt.{item} = src.{item}" for item in table_config[table]["match_keys"]]
        )
        sql_code = f"""
                MERGE INTO {table_prefix}{table} as tgt
                USING 
                    (SELECT * 
                    FROM {table_config[table]["catalogue_schema"]}.{table}
                    WHERE CAST({table_config[table]["load_date_column"]} as timestamp) {operator} '{filter_date}'
                    ) as src
                ON {join_condition}
                WHEN NOT MATCHED THEN 
                    INSERT *
                """
        df = spark.sql(sql_code)


def duplicate_data_loader_append(
    spark,
    table_config: dict,
    tables: list,
    table_prefix: str,
    filter_date: str,
    overwrite_date: str,
):
    if overwrite_date == filter_date:
        print("overwrite date must different to filter date")
        return
    for table in tables:
        col_order = [col for col in spark.table(f"{table_prefix}{table}").columns]
        df = spark.sql(
            f"""
                       SELECT * 
                       FROM {table_prefix}{table}
                       WHERE CAST({table_config[table]["load_date_column"]} as date) = '{filter_date}'
                       """
        )
        df = df.withColumn(
            table_config[table]["load_date_column"],
            f.to_timestamp(f.lit(overwrite_date)),
        )
        df = df.select(*col_order)
        df.write.mode("append").saveAsTable(f"{table_prefix}{table}")


def new_data_loader(
    spark,
    table_config: dict,
    tables: list,
    table_prefix: str,
    filter_date: str,
    overwrite_date: str,
):
    if overwrite_date == filter_date:
        print("overwrite date must different to filter date")
        return
    for table in tables:

        col_order = [col for col in spark.table(f"{table_prefix}{table}").columns]

        df = spark.sql(
            f"""
                       SELECT * 
                       FROM {table_prefix}{table}
                       WHERE CAST({table_config[table]["load_date_column"]} as date) = '{filter_date}'
                       """
        )
        df = df.withColumn(
            table_config[table]["load_date_column"],
            f.to_timestamp(f.lit(overwrite_date)),
        )

        if table_config[table]["type"] == "kpis":
            for kpi_col in table_config[table]["kpi_keys"]:
                df = df.withColumn(
                    kpi_col,
                    f.when(f.col(kpi_col).isNull(), 1).otherwise(200 * f.col(kpi_col)),
                )
        else:
            for kpi_col in table_config[table]["kpi_keys"]:
                df = df.withColumn(
                    kpi_col,
                    f.when(f.col(kpi_col).isNull(), "test").otherwise(f.lit("blah")),
                )

        df = df.select(*col_order)
        df.write.mode("append").saveAsTable(f"{table_prefix}{table}")


def mixed_data_loader(
    spark,
    table_config: dict,
    tables: list,
    table_prefix: str,
    filter_date: str,
    overwrite_date: str,
):
    if overwrite_date == filter_date:
        print("overwrite date must different to filter date")
        return
    for table in tables:
        col_order = [col for col in spark.table(f"{table_prefix}{table}").columns]

        df = spark.sql(
            f"""
                       SELECT * 
                       FROM {table_prefix}{table}
                       WHERE CAST({table_config[table]["load_date_column"]} as date) = '{filter_date}'
                       """
        )
        df = df.withColumn(
            table_config[table]["load_date_column"],
            f.to_timestamp(f.lit(overwrite_date)),
        )

        for kpi_col in table_config[table]["kpi_keys"]:
            window = w.orderBy(table_config[table]["load_date_column"])
            df = df.withColumn("row_number", f.row_number().over(window))

            df = df.withColumn(
                kpi_col,
                f.when((f.col("row_number") % 2 == 0) & (f.col(kpi_col).isNull()), 1)
                .when(
                    (f.col("row_number") % 2 == 0) & (f.col(kpi_col).isNotNull()),
                    f.col(kpi_col) * 200,
                )
                .otherwise(f.col(kpi_col)),
            )
            df = df.drop("row_number")

        df = df.select(*col_order)
        df.write.mode("append").saveAsTable(f"{table_prefix}{table}")


def add_columns(
    spark,
    table_config: dict,
    tables: list,
    table_prefix: str,
    filter_date: str,
    overwrite_date: str,
):
    for table in tables:
        sql_code = f"""
        
            ALTER TABLE {table_prefix}{table}
            ADD COLUMN NEW_COLUMN INT;
        """
        spark.sql(sql_code)

        new_col = f"""
            SELECT * FROM {table_prefix}{table}
            WHERE CAST(TIMESTAMP AS DATE) = '{filter_date}'
        """
        new_col_df = spark.sql(new_col)
        new_col_df = new_col_df.withColumn("NEW_COLUMN", f.lit(123))
        new_col_df = new_col_df.withColumn(
            table_config[table]["load_date_column"],
            f.to_timestamp(f.lit(overwrite_date)),
        )

        new_col_df.write.mode("append").saveAsTable(f"{table_prefix}{table}")


def drop_columns(
    spark,
    table_config: dict,
    tables: list,
    table_prefix: str,
    filter_date: str,
    overwrite_date: str,
):
    for table in tables:
        sql_code = f"""
            ALTER TABLE {table_prefix}{table}
            DROP COLUMN Op;
        """

        spark.sql(sql_code)

        new_col = f"""
            SELECT * FROM {table_prefix}{table}
            WHERE CAST(TIMESTAMP AS DATE) = '{filter_date}'
        """
        dropped_col_df = spark.sql(new_col)
        dropped_col_df = dropped_col_df.withColumn(
            table_config[table]["load_date_column"],
            f.to_timestamp(f.lit(overwrite_date)),
        )

        dropped_col_df.write.mode("append").saveAsTable(f"{table_prefix}{table}")


def null_data_loader(
    spark,
    table_config: dict,
    tables: list,
    table_prefix: str,
    filter_date: str,
    overwrite_date: str,
):
    if overwrite_date == filter_date:
        print("overwrite date must different to filter date")
        return
    for table in tables:

        col_order = [col for col in spark.table(f"{table_prefix}{table}").columns]

        df = spark.sql(
            f"""
                       SELECT * 
                       FROM {table_prefix}{table}
                       WHERE CAST({table_config[table]["load_date_column"]} as date) = '{filter_date}'
                       """
        )
        df = df.withColumn(
            table_config[table]["load_date_column"],
            f.to_timestamp(f.lit(overwrite_date)),
        )

        cols_to_update = [
            col for col in table_config[table]["match_keys"] if col != "timestamp"
        ]
        num_segments = len(cols_to_update) + 1
        segment_size = df.count() // num_segments

        base_df = df.limit(segment_size)

        # Define a window specification
        window_spec = Window.orderBy(table_config[table]["load_date_column"])
        # Add a row_number column to the DataFrame
        df_with_row_numbers = df.withColumn(
            "row_number", f.row_number().over(window_spec)
        )

        for i, col_name in enumerate(cols_to_update):
            start = segment_size + (i * segment_size)
            end = start + segment_size

            segment = df_with_row_numbers.filter(
                (f.col("row_number") >= start) & (f.col("row_number") < end)
            )
            segment = segment.withColumn(col_name, f.lit(None))
            segment = segment.drop("row_number")
            base_df = base_df.union(segment)

        base_df = base_df.withColumn(
            table_config[table]["load_date_column"],
            f.to_timestamp(f.lit(overwrite_date)),
        )
        result_df = base_df.select(*col_order)
        result_df.write.mode("append").saveAsTable(f"{table_prefix}{table}")


def table_delete(spark, tables: list, table_prefix: str):
    for table in tables:
        try:
            spark.sql(f"DROP TABLE {table_prefix}{table}")
            print(f">>> Table {table_prefix}{table} dropped")
        except Exception as e:
            print(e)


def full_copy(spark, tables: list, table_prefix: str, source_schema: str):
    for table in tables:
        spark.sql(
            f"""
                    MERGE INTO {table_prefix}{table} tgt
                    USING {source_schema}.{table} src
                    ON tgt.id = src.id
                    WHEN NOT MATCHED THEN INSERT *
                    """
        )
