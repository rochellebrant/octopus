import json
import sys
import os
import importlib
import traceback
import pyspark.sql.functions as f
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Databricks Pipeline").getOrCreate()

try:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
except ImportError:
    pass

def import_sql_scripts(path_to_models, models_dict):
    # Ensure the path is in sys.path for imports
    if path_to_models not in sys.path:
        sys.path.append(path_to_models)
        print(f"Added {path_to_models} to sys.path")

    for filename in os.listdir(path_to_models):
        if (
            filename.endswith(".py")
            and filename != "__init__.py"
            and "parameters" not in filename
        ):
            module_name = filename[:-3]  # Remove '.py' extension

            try:
                print(f"Importing module: {module_name}")
                module = importlib.import_module(module_name)

                # Find the single non-dunder attribute dynamically that is ACTUALLY a string!
                attributes = [
                    attr for attr in dir(module) 
                    if not attr.startswith("__") and isinstance(getattr(module, attr), str)
                ]

                if len(attributes) == 1:
                    attr_name = attributes[0]  # Get the attribute name
                    models_dict[module_name] = getattr(module, attr_name)
                    print(f"Stored {attr_name} from {module_name} in models_dict[{module_name}].")
                elif len(attributes) > 1:
                    print(f"Warning: Multiple attributes found in {module_name}, using the first one.")
                    attr_name = attributes[0]
                    models_dict[module_name] = getattr(module, attr_name)
                else:
                    print(f"Warning: No valid attributes found in {module_name}.")

                # Store the full module in globals() for direct access
                globals()[module_name] = module

            except Exception as e:
                print(f"Error importing module {module_name} from filepath {path_to_models}: {e}")

def setup_prefix(layer, development_prefix, source_database, bronze_database, silver_database, gold_database):
    prefix = '' if development_prefix == 'production' else development_prefix

    result = {
        "prefix": prefix,
        "raw_tbl_prefix": None,
        "bronze_tbl_prefix": None,
        "silver_entry_tbl_prefix": None,
        "silver_tbl_prefix": None,
        "gold_entry_tbl_prefix": None,
    }

    if layer == "bronze_dlt":
        result["raw_tbl_prefix"] = f"{source_database}.{prefix}"
        result["bronze_tbl_prefix"] = f"{bronze_database}.{prefix}bronze_"

    elif layer == "silver_entry":
        result["bronze_tbl_prefix"] = f"{bronze_database}.{prefix}bronze_"
        result["silver_entry_tbl_prefix"] = f"{silver_database}.{prefix}silver_model_"

    elif layer == "silver_dlt":
        result["silver_entry_tbl_prefix"] = f"{silver_database}.{prefix}silver_model_"
        result["silver_tbl_prefix"] = f"{silver_database}.{prefix}silver_"

    elif layer == "gold_entry":
        result["bronze_tbl_prefix"] = f"{bronze_database}.{prefix}bronze_"
        result["silver_tbl_prefix"] = f"{silver_database}.{prefix}silver_"
        result["gold_entry_tbl_prefix"] = f"{gold_database}.{prefix}gold_model_"

    return result

def init_entry_table_common(
    spark,
    model: str,
    script: str,
    target_tbl_prefix: str,
    create_mode: str,
    bronze_tbl_prefix: str = None,
    silver_tbl_prefix: str = None,
    gold_tbl_prefix: str = None
):
    query = script.format(
        bronze_prefix=bronze_tbl_prefix,
        silver_prefix=silver_tbl_prefix,
        gold_prefix=gold_tbl_prefix,
    )
    table_name = f"{target_tbl_prefix}{model}"
    temp_view = f"{model}_temp"

    try:
        df = spark.sql(f"SELECT * FROM ({query}) WHERE 1 = 0")
        df.createOrReplaceTempView(temp_view)
        print(f"\t🆕 Created empty temp view: {temp_view}")
    except Exception as e:
        raise Exception(f"\t> ⚠️ Error creating temp view {temp_view}: {e}") from e

    try:
        spark.sql(f"{create_mode} {table_name} AS SELECT * FROM {temp_view}")
        print(f"\t📦 Table created with mode {create_mode}: {table_name}")
    except Exception as e:
        raise Exception(f"\t> ⚠️ Error creating table {table_name}: {e}") from e

    try:
        spark.sql(f"GRANT MANAGE ON {table_name} TO `Sennen XIO Project`")
        print(f"\t🔐 Granted MANAGE permission on {table_name}")
    except Exception as e:
        raise Exception(f"\t> ⚠️ Failed to grant permission on {table_name}: {e}") from e

def check_if_table_is_empty(spark, target_tbl_prefix: str, table: str, models_config):
    table_name = f"{target_tbl_prefix}{table}"

    if table not in models_config:
        raise KeyError(f">>> ⚠️ Table '{table}' not found in models_config.")

    timestamp_col = models_config[table].get("load_date_column")
    if not timestamp_col:
        raise KeyError(f">>> ⚠️ 'load_date_column' parameter missing for table '{table}'.")

    query = f"""
        SELECT 
            COUNT(*) AS row_count, 
            MAX({timestamp_col}) AS max_timestamp
        FROM {table_name}
    """
    try:
        result = spark.sql(query).collect()[0]
        row_count = result["row_count"]
        max_timestamp = result["max_timestamp"]

        if row_count == 0:
            max_timestamp = None

        return row_count, max_timestamp

    except Exception as e:
        print(f">>> ⚠️ Error in check_if_table_is_empty() for table {table_name}: {str(e)}")
        raise e

def load_entry_tables(
    spark,
    models_config,
    table: str,
    script: str,
    target_tbl_prefix: str,
    bronze_tbl_prefix: str = None,
    silver_tbl_prefix: str = None,
    gold_tbl_prefix: str = None
) -> None:
    if table not in models_config:
        raise KeyError(f"\t⚠️ Table '{table}' not found in models_config.")

    query = script.format(
        bronze_prefix=bronze_tbl_prefix,
        silver_prefix=silver_tbl_prefix,
        gold_prefix=gold_tbl_prefix,
    )
    table_name = f"{target_tbl_prefix}{table}"
    
    table_type = models_config[table].get("table_type", "merge")
    
    if table_type == "replace":
        print(f"\t📤 Inserting all records to replaced table {table_name}")
        sql_code = f"INSERT INTO {table_name} SELECT DISTINCT * FROM ({query})"
        try:
            spark.sql(sql_code)
        except Exception as e:
            err_msg = f">>> ⚠️ Error inserting into: {table_name} \n Exception: {e}"
            print(err_msg)
            raise RuntimeError(err_msg) from e
        return

    missing_columns, missing_match_keys = [], []
    
    raw_match_keys = models_config[table].get("match_keys")
    if not raw_match_keys:
        raise KeyError(f"\t> ⚠️ 'match_keys' missing in config for table '{table}'. Required for merge operations.")
        
    match_keys = raw_match_keys + [models_config[table]["load_date_column"]]
    existing_table_columns = [col.name for col in spark.catalog.listColumns(table_name)]

    for key in match_keys:
        if key not in existing_table_columns:
            missing_columns.append(key)
            
    if missing_columns:
        err_msg = (
            f"\n>>> ⚠️ Column(s) '{', '.join(missing_columns)}' in match_keys does not exist in '{table_name}'.\n"
            f"Please verify they exist in your SQL select, then DROP TABLE {table_name} and rerun."
        )
        print(err_msg)
        raise KeyError(err_msg)

    cnt, max_timestamp = check_if_table_is_empty(spark, target_tbl_prefix, table, models_config)

    if cnt > 0:
        where_clause = f"WHERE {models_config[table]['load_date_column']} > '{max_timestamp}'"
        print(f"\t📤 Loading new records only to {table_name}")
    else:
        where_clause = "WHERE 1 = 1"
        print(f"\t📤 Loading all records to {table_name}")

    match_conditions = " AND ".join([f"tgt.{col} = src.{col}" for col in match_keys])
    sql_code = f"""
            MERGE INTO {table_name} tgt
            USING (SELECT DISTINCT * FROM ({query}) {where_clause}) src
            ON {match_conditions}
            WHEN NOT MATCHED THEN INSERT *
    """

    try:
        spark.sql(sql_code)
    except Exception as e:
        err_msg = f">>> ⚠️ Error merging into: {table_name}\n Merge conditions: {match_conditions}\n Exception: {e}"
        print(err_msg)
        raise RuntimeError(err_msg) from e

def process_table(
    spark,
    table_configuration,
    models_dict,
    path_to_models,
    tgt_table,
    failed_tables,
    target_tbl_prefix,
    bronze_tbl_prefix: str = None,
    silver_tbl_prefix: str = None,
    gold_tbl_prefix: str = None
):
    try:
        if table_configuration[tgt_table]["code_type"] == "sql":
            file_name = table_configuration[tgt_table]["file_name"]
            if len(file_name) < 3:
                raise ValueError(f"Invalid file_name '{file_name}' for table '{tgt_table}'.")

            script_name = file_name[:-3]
            script = models_dict[script_name]
            
            # Safely fetch table_type to avoid KeyError
            tbl_type = table_configuration[tgt_table].get("table_type", "merge")
            create_mode = "CREATE OR REPLACE TABLE" if tbl_type == "replace" else "CREATE TABLE IF NOT EXISTS"
            
            init_entry_table_common(
                spark = spark,
                model = tgt_table,
                script = script,
                create_mode = create_mode,
                bronze_tbl_prefix = bronze_tbl_prefix,
                silver_tbl_prefix = silver_tbl_prefix,
                gold_tbl_prefix = gold_tbl_prefix,
                target_tbl_prefix = target_tbl_prefix
            )

            load_entry_tables(
                spark = spark,
                table = tgt_table,
                script = script,
                models_config = table_configuration,
                bronze_tbl_prefix = bronze_tbl_prefix,
                silver_tbl_prefix = silver_tbl_prefix,
                gold_tbl_prefix = gold_tbl_prefix,
                target_tbl_prefix = target_tbl_prefix
            )
            print(f"\t✅ Completed {target_tbl_prefix}{tgt_table}")

        elif table_configuration[tgt_table]["code_type"] == "notebook":
            repo_root = os.getcwd()
            notebook_relative_path = os.path.relpath(
                os.path.join(path_to_models, table_configuration[tgt_table]["file_name"]),
                repo_root,
            ).replace(".ipynb", "")
            
            notebook_params = dict(table_configuration[tgt_table].get("notebook_params", {}))
            args = {
                "notebook_params": json.dumps(notebook_params),
                "tgt_table": tgt_table or "",
                "bronze_tbl_prefix": bronze_tbl_prefix or "",
                "silver_tbl_prefix": silver_tbl_prefix or "",
                "gold_tbl_prefix": gold_tbl_prefix or "",
                "target_tbl_prefix": target_tbl_prefix or "",
            }

            print(f"📂 Running notebook (relative): {notebook_relative_path}")
            dbutils.notebook.run(notebook_relative_path, timeout_seconds=1200, arguments=args)

        else:
            raise ValueError(f"Invalid code_type '{table_configuration[tgt_table]['code_type']}' for table '{tgt_table}'.")

    except Exception as e:
        failed_tables.append(tgt_table)
        print(f">>> ⚠️ Error in process_table() for table {tgt_table}: {e}")
        raise # Reraise the exception so the thread executor correctly catches the failure

class TableProcessingError(RuntimeError):
    pass

def abort_if_failures(failed_tables, context: str | None = None) -> None:
    if failed_tables:
        ctx = f" [{context}]" if context else ""
        failures = sorted(set(failed_tables))
        num_failures = len(failures)
        raise TableProcessingError(
            f"🚨 {num_failures} table(s) failed{ctx}. "
            f"Failing the job/notebook. Failed tables: {', '.join(failures)}"
        )

def entry_table_loader(
    spark,
    max_level,
    table_configuration,
    models_dict,
    path_to_models,
    target_tbl_prefix,
    bronze_tbl_prefix=None,
    silver_tbl_prefix=None,
    gold_tbl_prefix=None
):
    all_failures = []   
    
    for level in range(1, max_level + 1):
        tables_to_run = [
            t for t in table_configuration
            if table_configuration[t]["level"] == level
            ]
        
        if not tables_to_run:
            continue
        
        failed_tables = []
        successful_tables = []

        with ThreadPoolExecutor(max_workers=len(tables_to_run)) as executor:
            future_to_table = {
                executor.submit(
                    process_table,
                    spark,
                    table_configuration,
                    models_dict,
                    path_to_models,
                    t,
                    failed_tables,
                    target_tbl_prefix,
                    bronze_tbl_prefix,
                    silver_tbl_prefix,
                    gold_tbl_prefix
                ): t
                for t in tables_to_run
            }

            for future in as_completed(future_to_table):
                table = future_to_table[future]
                try:
                    future.result()
                    if table not in failed_tables:
                        successful_tables.append(table)
                except Exception as e:
                    print(f">>> ⚠️ Exception occurred while processing {table}:\n{traceback.format_exc()}")
                    if table not in failed_tables:
                        failed_tables.append(table)

        all_failures.extend(failed_tables)

        print('-'*80)
        print(f"Level {level} Processing Summary:")
        print(f"\tProcessed: {successful_tables}" if successful_tables else "\t❌ No tables were successfully processed.")
        print(f"\t❌ Failures: {failed_tables}" if failed_tables else "\t No failures.")
        print('-'*80)

    abort_if_failures(all_failures, context="Full pipeline run")