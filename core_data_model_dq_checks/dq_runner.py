# Databricks notebook source
# # CONFIG & WIDGETS
# dbutils.widgets.text("bundle_target", "dev")
# dbutils.widgets.text("bundle_root_path", "")
# dbutils.widgets.text("source_catalog", "oegen_data_prod_prod")
# dbutils.widgets.text("source_schema", "core_data_model_dev")
# dbutils.widgets.text("dq_catalog", "oegen_data_prod_prod")
# dbutils.widgets.text("dq_schema", "core_data_model_dq_dev")
# dbutils.widgets.text("dq_tbl_prefix", "bronze")
# dbutils.widgets.text("dq_config_home", "core_data_model/core_data_model_dq_checks")
# dbutils.widgets.text("config_rel_path", "bronze")
# dbutils.widgets.text("common_path", "common")
# dbutils.widgets.text("tables_include", "*") 
# dbutils.widgets.text("tables_exclude", "")
# dbutils.widgets.dropdown("run_type", "all", ["all", "standard", "bespoke"])

# COMMAND ----------

# MAGIC %pip install --upgrade databricks-labs-dqx 'databricks-labs-dqx[llm]'
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from fnmatch import fnmatch
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, FloatType, ArrayType

BUNDLE_TARGET = dbutils.widgets.get("bundle_target").strip()
COMMON_PATH = dbutils.widgets.get("common_path").strip()
CONFIG_REL_PATH = dbutils.widgets.get("config_rel_path").strip()
DQ_CONFIG_HOME = dbutils.widgets.get("dq_config_home").strip()
INCLUDE = dbutils.widgets.get("tables_include").strip()
EXCLUDE = dbutils.widgets.get("tables_exclude").strip()
RUN_TYPE = dbutils.widgets.get("run_type").strip().lower()

source_db = f"{dbutils.widgets.get('source_catalog')}.{dbutils.widgets.get('source_schema')}"
dq_db     = f"{dbutils.widgets.get('dq_catalog')}.{dbutils.widgets.get('dq_schema')}"
dq_tbl_prefix = dbutils.widgets.get("dq_tbl_prefix")

runtime = {
    "source_db": source_db,
    "dq_db": dq_db,
    "report_table": f"{dq_db}.{dq_tbl_prefix}_dq_summary"
}

summary_schema = StructType([
    StructField("emoji", StringType(), True),
    StructField("check_type", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("check_names", ArrayType(StringType()), True),
    StructField("status", StringType(), True),
    StructField("success_rate", FloatType(), True),
    StructField("failed_rows", LongType(), True),
    StructField("warning_rows", LongType(), True),
    StructField("total_rows", LongType(), True),
    StructField("checks_run", LongType(), True),
    StructField("issues_table", StringType(), True),
    StructField("run_id", StringType(), True),
])

# COMMAND ----------

# DBTITLE 1,import libraries
if BUNDLE_TARGET == "dev":

    def find_repo_root(path, markers=['.github']):
        while path != os.path.dirname(path):
            if any(marker in os.listdir(path) for marker in markers):
                return path
            path = os.path.dirname(path)
        return None
    
    current_dir = os.getcwd()
    REPO_ROOT = find_repo_root(current_dir)

    if REPO_ROOT:
        if REPO_ROOT not in sys.path:
            sys.path.append(REPO_ROOT)

        path_to_funcs = os.path.join(REPO_ROOT, COMMON_PATH)
        if COMMON_PATH not in sys.path and os.path.exists(path_to_funcs):
            sys.path.append(path_to_funcs)
        print(f"✅ Appended common libs from: {path_to_funcs}")
        
    else:
        raise FileNotFoundError("Could not find the repository root. Ensure databricks.yml exists at the root.")

else:
    REPO_ROOT = dbutils.widgets.get("bundle_root_path").strip()
    if COMMON_PATH and os.path.exists(COMMON_PATH):
        sys.path.append(COMMON_PATH)
        print(f"✅ Appended common path: {COMMON_PATH}")
    else:
        raise FileNotFoundError(f"Common path not found: {COMMON_PATH}")

from libraries.dq_framework.engine.file_selection import discover_file_names, parse_patterns
from libraries.dq_framework.engine.runner import DQRunner

config_base_dir = os.path.join(REPO_ROOT, DQ_CONFIG_HOME)
tables_config_path = os.path.join(REPO_ROOT, DQ_CONFIG_HOME, "configs", CONFIG_REL_PATH)
print(f"Checking for configs in: {tables_config_path}")
print(f"Files found: {os.listdir(tables_config_path) if os.path.exists(tables_config_path) else 'Folder Missing'}")

# COMMAND ----------

# DISCOVERY - Phase 1: YAML Configs
try:
    yaml_tables = discover_file_names(
        config_base_dir=config_base_dir,
        config_rel_path=CONFIG_REL_PATH,
        extension="yaml",
        include=INCLUDE,
        exclude=EXCLUDE
    )
except ValueError:
    yaml_tables = []
    
print(f"Discovered {len(yaml_tables)} tables for /{CONFIG_REL_PATH}: {yaml_tables}")

# DISCOVERY - Phase 2: Bespoke Notebooks (Now searching subfolders)
bespoke_nb_dir = os.path.join(REPO_ROOT, DQ_CONFIG_HOME, "notebooks", CONFIG_REL_PATH)

def discover_bespoke_notebooks(path, include_str, exclude_str):
    if not os.path.exists(path):
        return {}
        
    include_pats = parse_patterns(include_str) or ["*"]
    exclude_pats = parse_patterns(exclude_str)
    
    bespoke_dict = {}
    for item in os.listdir(path):
        full_path = os.path.join(path, item)
        if os.path.isdir(full_path):
            table_name = item
            if any(fnmatch(table_name, pattern) for pattern in include_pats):
                if not (exclude_pats and any(fnmatch(table_name, pattern) for pattern in exclude_pats)):
                    # Grab all python/jupyter files in the table's subfolder
                    nbs = [f.split('.')[0] for f in os.listdir(full_path) if f.endswith(('.py', '.ipynb'))]
                    if nbs:
                        bespoke_dict[table_name] = sorted(nbs)
                        
    return bespoke_dict

bespoke_tables_dict = discover_bespoke_notebooks(bespoke_nb_dir, INCLUDE, EXCLUDE)
bespoke_tables_list = sorted(list(bespoke_tables_dict.keys()))

# The "Super Set" of all tables involved in DQ
all_tables = sorted(list(set(yaml_tables + bespoke_tables_list)))

print(f"🔎 Discovery Summary:")
print(f"   - YAML Tables: {len(yaml_tables)}")
print(f"   - Bespoke Tables: {len(bespoke_tables_list)} (Total {sum(len(v) for v in bespoke_tables_dict.values())} notebooks)")

if not all_tables:
    print(f"⚠️ No tables matched include='{INCLUDE}' and exclude='{EXCLUDE}'. Exiting cleanly.")
    dbutils.notebook.exit("No tables to process")

# COMMAND ----------

# DBTITLE 1,both
# PARALLEL EXECUTION
runner = DQRunner(spark, runtime, config_base_dir)
results_list_of_dict = []

# arguments to pass into child notebooks
notebook_args = {
    "bundle_target": dbutils.widgets.get("bundle_target"),
    "bundle_root_path": dbutils.widgets.get("bundle_root_path"),
    "source_catalog": dbutils.widgets.get("source_catalog"),
    "source_schema": dbutils.widgets.get("source_schema"),
    "dq_catalog": dbutils.widgets.get("dq_catalog"),
    "dq_schema": dbutils.widgets.get("dq_schema"),
    "dq_tbl_prefix": dbutils.widgets.get("dq_tbl_prefix"),
    "DQ_CONFIG_HOME": dbutils.widgets.get("dq_config_home"),
    "CONFIG_REL_PATH": dbutils.widgets.get("config_rel_path"),
    "common_path": COMMON_PATH,
    "run_id": "BESPOKE"
}

workspace_base_path = REPO_ROOT.replace("/Workspace", "")
bespoke_rel_dir = os.path.join(workspace_base_path, DQ_CONFIG_HOME, "notebooks", CONFIG_REL_PATH)

with ThreadPoolExecutor(max_workers=5) as executor:
    future_to_info = {}

    for table in all_tables:
        # Trigger Standard Check if YAML exists AND run_type allows it
        if table in yaml_tables and RUN_TYPE in ["all", "standard"]:
            future = executor.submit(runner.run_for_table, CONFIG_REL_PATH, table)
            future_to_info[future] = {"table": table, "type": "STANDARD", "check_name": None}

        # Trigger Bespoke Notebooks if they exist AND run_type allows it
        if table in bespoke_tables_dict and RUN_TYPE in ["all", "bespoke"]:
            for nb_name in bespoke_tables_dict[table]:
                nb_path = f"/{bespoke_rel_dir}/{table}/{nb_name}"
                
                # Define the exact output table name right here
                issues_fqtn = f"{dq_db}.bespoke__{table}__{nb_name}"
                
                current_args = notebook_args.copy()
                current_args["table_name"] = table
                current_args["check_name"] = nb_name
                current_args["issues_fqtn"] = issues_fqtn
                
                future = executor.submit(dbutils.notebook.run, nb_path, 600, current_args)
                
                # Store the issues_fqtn in the info dict so the result checker can use it
                future_to_info[future] = {
                    "table": table, 
                    "type": "BESPOKE", 
                    "check_name": nb_name,
                    "issues_fqtn": issues_fqtn 
                }

    for future in as_completed(future_to_info):
        info = future_to_info[future]
        table = info["table"]
        check_type = info["type"]
        check_name = info["check_name"]
        
        try:
            if check_type == "STANDARD":
                data = future.result()
                if data:
                    data["check_type"] = "STANDARD"
                    if "check_names" not in data:
                        data["check_names"] = ["Standard Checks"]
                    results_list_of_dict.append(data)
            
            else:
                # BESPOKE result logic
                future.result() 
                
                # Grab the exact table name we told the notebook to use
                bespoke_fqtn = info["issues_fqtn"] 
                failed_row_count = 0
                
                try:
                    # Check the table
                    df = spark.table(bespoke_fqtn)
                    failed_row_count = df.count()
                except Exception:
                    # If it doesn't exist, the notebook dropped it (0 issues)
                    failed_row_count = 0
                
                status = "Issues Found" if failed_row_count > 0 else "Success"
                emoji = "❌" if failed_row_count > 0 else "✅"

                results_list_of_dict.append({
                    "emoji": emoji,
                    "check_type": "BESPOKE",
                    "table_name": table,
                    "check_names": [check_name],
                    "status": status,
                    "success_rate": None,
                    "failed_rows": failed_row_count, 
                    "warning_rows": None,
                    "total_rows": None,
                    "checks_run": 1,
                    "issues_table": f'SELECT * FROM {bespoke_fqtn}',
                    "run_id": "BESPOKE"
                })

        except Exception as exc:
            results_list_of_dict.append({
                "emoji": "🚨",
                "check_type": check_type,
                "table_name": table,
                "check_names": [check_name] if check_name else ["STANDARD"],
                "status": "System Error",
                "success_rate": None,
                "failed_rows": None,
                "warning_rows": None,
                "total_rows": None,
                "checks_run": 0,
                "issues_table": str(exc),
                "run_id": "FAILED"
            })

# COMMAND ----------

# SUMMARY & CIRCUIT BREAKER
if results_list_of_dict:
    new_results_df = spark.createDataFrame(results_list_of_dict, schema=summary_schema)
    report_table = runtime['report_table']
    
    if spark.catalog.tableExists(report_table):
        existing_df = spark.table(report_table)
        tables_just_run = [r["table_name"] for r in results_list_of_dict]
        
        # Need to be careful here: if we only ran a subset of bespoke checks, 
        # overwriting the whole table's history might erase the other bespoke checks
        # But keeping it simple for now: overwrite all records for the tables that were just processed.
        filtered_df = existing_df.filter(~F.col("table_name").isin(tables_just_run))
        final_df = filtered_df.unionByName(new_results_df, allowMissingColumns=True)
    else:
        final_df = new_results_df
        
    final_df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(report_table)
    
    print(f"✅ Processed {len(results_list_of_dict)} task(s) and updated {report_table}")
    display(new_results_df.orderBy(F.desc("status"), "table_name"))
    
    system_errors = [r for r in results_list_of_dict if r['status'] == "System Error"]
    system_error_count = len(system_errors)

    if system_error_count > 0:
        failed_tables = ", ".join([f"{r['table_name']} ({r['check_names'][0]})" for r in system_errors])
        error_msg = f"System Failure: {system_error_count} processing error(s) in: {failed_tables}"
        print(f"❌ CRITICAL: {error_msg}")
        raise Exception(error_msg) 
    
    print("🌟 All DQ checks executed without system errors.")
else:
    raise Exception("⚠️ No tables were discovered or processed.")

# COMMAND ----------

