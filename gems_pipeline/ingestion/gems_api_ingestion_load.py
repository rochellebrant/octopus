# Databricks notebook source
# MAGIC %run ../utils/utils

# COMMAND ----------

# DBTITLE 1,Standard imports
import requests
import pyspark.sql.functions as f
import datetime
from dataclasses import dataclass, field

# COMMAND ----------

# DBTITLE 1,Pipeline folder name
FEAT_NONBUNDLE_PIPELINE_FOLDER_NAME = "gems_pipeline"
FEAT_BUNDLE_PIPELINE_FOLDER_NAME = ".bundle/xio-gems/dev/files"
REMOTE_PIPELINE_FOLDER_NAME = "xio-gems/files"

# COMMAND ----------

ENV = dbutils.widgets.get("ENV").strip().lower()
CATALOG = dbutils.widgets.get("catalog").strip()
DATABASE = dbutils.widgets.get("database").strip()

SCHEMA = f"{CATALOG}.{DATABASE}"

# COMMAND ----------

# DBTITLE 1,Import libraries / params
# BASE_URL = "https://gemsuat.gemsemea.com"# if ENV != "" else "https://gems.gemsemea.com"
BASE_URL = "https://gems.gemsemea.com"
# env_var = "prod" if ENV == "" else "dev"
env_var = "prod"
prefix = f"{ENV}_" if ENV else ""
CLIENT_ID = dbutils.secrets.get(scope="XIO", key=f"gems_client_id_{env_var}")
CLIENT_SECRET = dbutils.secrets.get(scope="XIO", key=f"gems_client_secret_{env_var}")

TARGET_TABLES = {
    "API_People",
    "API_Appointment",
    "API_Entity",
    "API_Shareholdings",
    "API_Company_Address",
    "API_Member_Guarantees",
}

token = get_token(BASE_URL, CLIENT_ID, CLIENT_SECRET)
headers = {"Authorization": f"Bearer {token}"}

# COMMAND ----------

# MAGIC %md
# MAGIC # Workflow State Management
# MAGIC
# MAGIC This notebook uses dataclasses to manage state in a structured, type-safe way instead of deeply nested dictionaries.
# MAGIC
# MAGIC ## Data Structure
# MAGIC
# MAGIC **`LandingTableState`** - Represents a single table's data:
# MAGIC * `table_id`: The saved search counter ID from GEMS
# MAGIC * `table_name`: Name of the table (e.g., "API_People", "OEGEN_Entity")
# MAGIC * `pages_count`: Number of API pages fetched
# MAGIC * `landing_layer_df`: Raw variant DataFrame from API responses
# MAGIC * `landing_layer_df_to_save`: Processed DataFrame with SCD columns and hashing
# MAGIC
# MAGIC **`SchemaState`** - Represents all data for a schema (e.g., "Person Basics"):
# MAGIC * `primarySchema`: Schema name
# MAGIC * `schema_data`: Raw variant DataFrame of all tables in the schema
# MAGIC * `filtered_results_df`: Filtered DataFrame containing only target tables
# MAGIC * `saved_search`: List of (table_id, table_name) tuples to process
# MAGIC * `landing_tables`: Dictionary mapping table names to `LandingTableState` objects
# MAGIC
# MAGIC **`WorkflowState`** - Top-level container:
# MAGIC * `schemas`: Dictionary mapping schema names to `SchemaState` objects
# MAGIC
# MAGIC ## Access Pattern
# MAGIC ```python
# MAGIC # Instead of: RESULTS_DICT[schema]["landing_tables"][table]["landing_layer_df"]
# MAGIC # Use: state.schemas[schema].landing_tables[table].landing_layer_df
# MAGIC ```
# MAGIC
# MAGIC This provides IDE autocomplete, type checking, and clearer code structure.

# COMMAND ----------

@dataclass
class LandingTableState:
    table_id: str
    table_name: str
    pages_count: int = 0
    landing_layer_df: object = None
    landing_layer_df_to_save: object = None

@dataclass
class SchemaState:
    primarySchema: str
    schema_data: object = None
    filtered_results_df: object = None
    saved_search: object = None
    landing_tables: dict[str, LandingTableState] = field(default_factory=dict)

@dataclass
class WorkflowState:
    schemas: dict[str, SchemaState] = field(default_factory=dict)

state = WorkflowState()

# COMMAND ----------

# MAGIC %md
# MAGIC # List out all schemas
# MAGIC
# MAGIC In this step we generate a list of all the schemas accessible via the API and filter for thsoe we want (note that this lets us check that the schemas we want actually exist in GEMS)

# COMMAND ----------

# Get every single schema from the API
schemas = requests.get(
    f"{BASE_URL}/gemswebapi/api/search",
    headers=headers,
).json()

# Extract just the schema names safely
all_schema_names = [item.get("primarySchema") for item in schemas if item.get("primarySchema")]

names_to_process = set()
print(f"Dynamically scanning {len(all_schema_names)} schemas for our target tables...")

for schema_name in all_schema_names:
    response = requests.get(f"{BASE_URL}/gemswebapi/api/search/{schema_name}", headers=headers)
    
    # Only check successful responses
    if response.status_code == 200:
        tables_in_schema = response.json()
        
        # Ensure it's a valid list of tables and not an error dictionary
        if isinstance(tables_in_schema, list):
            for t in tables_in_schema:
                if isinstance(t, dict):
                    title = t.get('title', '')
                    
                    # If we find a target table (or one starting with API_), mark this schema for processing!
                    if title in TARGET_TABLES or title.startswith('API_'):
                        names_to_process.add(schema_name)
                        break

print(f"✅ Target tables found inside these schemas: {names_to_process}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Get data from the actual tables
# MAGIC
# MAGIC Simply fetching the data and adding basic metadata/load timestamps.

# COMMAND ----------

for primarySchema in names_to_process:
    # ---> This line right here creates the 'Person Basics' and 'Company Basics' key <---
    state.schemas[primarySchema] = SchemaState(primarySchema=primarySchema)
    print(f"Setting up state for {primarySchema}")

    # Fetch table lists and filter them
    df = get_table_list_from_schema(primarySchema)
    state.schemas[primarySchema].schema_data = df

    filtered_df = get_primary_schema_from_class(state, primarySchema)
    state.schemas[primarySchema].filtered_results_df = filtered_df
    
    # Get the saved search IDs
    saved_search = get_saved_search_counter_from_class(state, primarySchema)
    state.schemas[primarySchema].saved_search = saved_search

# COMMAND ----------

for primarySchema in names_to_process:
    schema_state = state.schemas[primarySchema]

    for (table_id, table_name) in schema_state.saved_search:
        print(f"Processing {primarySchema}: {table_id} / {table_name}")

        pages = fetch_all_pages_as_json_list(
            base_url=BASE_URL,
            schema=primarySchema,
            company_id=table_id,
            headers=headers,
        )

        records_variant_df = create_df_from_list(pages)
        pages_count = len(pages)
        print(f"...Found {pages_count} pages")

        # Keep it raw: Just add metadata and a load timestamp for DLT to sequence by
        landing_df = (
            records_variant_df
            .withColumn("table_id", f.lit(table_id))
            .withColumn("table_name", f.lit(table_name))
            .withColumn("primarySchema", f.lit(primarySchema))
            .withColumn("env", f.lit(ENV))
            .withColumn("pages", f.lit(pages_count))
            .withColumn("url", f.lit(f"{BASE_URL}/gemswebapi/api/search/{primarySchema}/{table_id}"))
            .withColumn("datalake_ingestion_timestamp", f.current_timestamp()) # <-- Crucial for DLT SCD2
        )

        schema_state.landing_tables[table_name] = LandingTableState(
            table_id=table_id,
            table_name=table_name,
            pages_count=pages_count,
            landing_layer_df=landing_df,
            landing_layer_df_to_save=landing_df, # Saving the raw DataFrame directly
        )
     

# COMMAND ----------

# MAGIC %md
# MAGIC # Save down the landing layer
# MAGIC Pure append-only insert. No merges, no SCD2 logic.

# COMMAND ----------

for primarySchema in names_to_process:
    for table in state.schemas[primarySchema].landing_tables:
        print(f"Saving Landing Table: {table}")

        df = state.schemas[primarySchema].landing_tables[table].landing_layer_df_to_save

        target_table_name = (
            prefix
            + "landing"
            # + primarySchema.lower().replace(" ", "_")
            + "_"
            + table.lower().replace(" ", "_")
        )

        qualified_target_table = f"{SCHEMA}.{target_table_name}"
        print(f"Writing to: {qualified_target_table}")

        (df.write
         .format("delta")
         .mode("append")
         .option("mergeSchema", "true")
         .saveAsTable(qualified_target_table))

# COMMAND ----------

