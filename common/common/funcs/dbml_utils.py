import pyspark.sql.functions as f
from .utils import get_table_schema

def generate_schemas(spark, table_list, fq_schema):
    """Generates DBML Table definition strings"""
    schema_strings = []

    for table_name in table_list:
        try:
            # Pass spark into the utility function
            df = get_table_schema(spark, fq_schema, table_name)

            df_pk = df.filter(f.col("data_type").contains("PRIMARY KEY"))
            pk = df_pk.select(
                f.regexp_extract(f.col("data_type"), r"`(.*?)`", 1).alias("pk")
            ).collect()
            pk_name = pk[0]["pk"] if pk else None

            delta_stats_exists = df.filter(f.col("col_name") == "").count() > 0
            if delta_stats_exists:
                df = df.withColumn("row_num", f.monotonically_increasing_id())
                cutoff_index = df.filter(f.col("col_name") == "").select("row_num").first()[0]
                df = df.filter(f.col("row_num") < cutoff_index).drop("row_num")

            df = df.filter((f.col("col_name").isNotNull()) & (f.col("col_name") != ""))
            df = df.fillna({"col_name": "", "data_type": ""})

            formatted_columns = df.withColumn(
                "formatted_col",
                f.when(
                    f.col("col_name") == pk_name,
                    f.concat_ws(" ", f.col("col_name"), f.col("data_type"), f.lit("[PK]"))
                ).otherwise(f.concat_ws(" ", f.col("col_name"), f.col("data_type")))
            )

            columns_list = [str(row["formatted_col"]) for row in formatted_columns.select("formatted_col").collect()]

            table_schema = f"Table {table_name} {{\n  " + "\n  ".join(columns_list) + "\n}"
            schema_strings.append(table_schema)
            
        except Exception as e:
            print(f"Error generating schema for {table_name}: {e}")
            
    return "\n\n".join(schema_strings)


def generate_foreign_key_references(spark, table_list, fq_schema):
    """Generates DBML Ref strings for relationships"""
    schema_strings = []

    for table_name in table_list:
        try:
            # Pass spark into the utility function
            df = get_table_schema(spark, fq_schema, table_name)
            
            df_fk = df.filter(f.col("data_type").contains("FOREIGN KEY"))

            if df_fk.count() == 0:
                continue

            df_fk = df_fk.withColumn("source_column", f.regexp_extract(f.col("data_type"), r"\(`(.*?)`\)", 1))
            df_fk = df_fk.withColumn("target_column", f.regexp_extract(f.col("data_type"), r".*\((`?)(\w+)\1\)$", 2))
            df_fk = df_fk.withColumn("target_table", f.regexp_extract(f.col("data_type"), r"`([^`]+)`\.`([^`]+)`\.`([^`]+)`", 3))

            df_fk = df_fk.withColumn(
                "reference_string",
                f.concat(
                    f.lit("Ref: "), f.lit(table_name + "."), f.col("source_column"),
                    f.lit(" > "), f.col("target_table"), f.lit("."), f.col("target_column")
                )
            )

            reference_strings = [row["reference_string"] for row in df_fk.select("reference_string").collect()]
            schema_strings.extend(reference_strings)
            
        except Exception as e:
            print(f"Error generating FK refs for {table_name}: {e}")

    return "\n".join(schema_strings)