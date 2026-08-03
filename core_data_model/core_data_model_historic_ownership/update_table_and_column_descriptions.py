# Databricks notebook source
import os
import sys
import importlib

# --- 1. Retrieve Widgets ---
# Use default values for safer local testing if needed
bundle_target = dbutils.widgets.get("bundle_target").strip()
bundle_name = dbutils.widgets.get("bundle_name").strip()
common_path = dbutils.widgets.get("common_path").strip()
table_definitions_import_path = dbutils.widgets.get("table_definitions_import_path").strip()
catalog = dbutils.widgets.get("catalog").strip()
database = dbutils.widgets.get("database").strip()

print(f"🚀 Target: {bundle_target} | Bundle: {bundle_name}")

# --- 2. Setup Common Libraries (Runs in both Dev & Prod) ---
common_funcs_path = os.path.join(common_path, "funcs") 

if os.path.exists(common_funcs_path):
    if common_funcs_path not in sys.path:
        sys.path.append(common_funcs_path)
        print(f"✅ Appended common libs: {common_funcs_path}")
else:
    # Fallback: try appending just common_path if 'funcs' doesn't exist
    if common_path not in sys.path and os.path.exists(common_path):
        sys.path.append(common_path)
        print(f"✅ Appended common libs: {common_path}")
    else:
        print(f"⚠️ Warning: Common path not found: {common_path}")

# --- 3. Setup Repo/Bundle Root for Table Definitions ---
if bundle_target == "dev":
    # In Dev, we need to find where the Repo is checked out locally
    def find_repo_root(path, markers=['databricks.yml', '.github']):
        while path != os.path.dirname(path):
            if any(marker in os.listdir(path) for marker in markers):
                return path
            path = os.path.dirname(path)
        return None
    
    current_dir = os.getcwd()
    repo_root = find_repo_root(current_dir)
    
    if repo_root:
        print(f"🌱 Repo Root (Dev): {repo_root}")
        if repo_root not in sys.path:
            sys.path.append(repo_root)
    else:
        raise FileNotFoundError("Could not find repository root (looked for databricks.yml).")

elif bundle_target in ("staging", "prod"):
    # In Prod, the bundle files are usually deployed to a specific root path
    repo_root = dbutils.widgets.get("bundle_root_path").strip()
    
    # We typically need to append the 'files' directory inside the bundle to find the modules
    # Assuming structure: /Workspace/.../bundle_name/files/
    prod_files_path = os.path.join(repo_root, bundle_name, "files")
    
    if os.path.exists(prod_files_path):
        if prod_files_path not in sys.path:
            sys.path.append(prod_files_path)
        print(f"🌱 Repo Root (Prod): {prod_files_path}")
    else:
        # Fallback to just repo_root if 'files' structure isn't used
        if repo_root not in sys.path:
            sys.path.append(repo_root)
            print(f"🌱 Repo Root (Prod): {repo_root}")

# --- 4. Validation ---
if not (catalog and database and table_definitions_import_path):
    raise ValueError("catalog, database, and table_definitions_import_path must all be provided")

# --- 5. Dynamic Import ---
# use 'table_definitions_import_path'.
try:
    print(f"🔄 Importing module: {table_definitions_import_path}")
    mod = importlib.import_module(table_definitions_import_path)
    column_definitions = getattr(mod, "column_definitions", None)
except ModuleNotFoundError as e:
    print(f"❌ Import Failed. Current sys.path: {sys.path}")
    raise e

if not isinstance(column_definitions, dict):
    raise TypeError(f"Module '{table_definitions_import_path}' must expose `column_definitions` as a dict")

# --- 6. Application Logic ---
from schema_metadata.applier import apply_comments_for_all_tables

summary = apply_comments_for_all_tables(
    spark=spark,
    catalog=catalog,
    database=database,
    column_definitions=column_definitions,
    dry_run=False,
    fail_fast=False,            
    fail_on_any_error=True,       
    fail_on_missing_table=False,  
    preflight_require_tables_exist=True, 
)

summary_df = summary.to_spark_df(spark)
display(summary_df)

# COMMAND ----------

