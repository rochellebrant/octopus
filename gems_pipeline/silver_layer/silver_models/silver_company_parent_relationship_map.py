# Databricks notebook source
# DBTITLE 1,imports
!pip install networkx

# COMMAND ----------

# --- 1. IMPORTS & ENV SETUP ---
import json
import os
import sys
import pandas as pd
import networkx as nx
from pyspark.sql import functions as f
from pyspark.sql.types import TimestampType

FEAT_NONBUNDLE_PIPELINE_FOLDER_NAME = "gems_pipeline"
FEAT_BUNDLE_PIPELINE_FOLDER_NAME = ".bundle/xio-gems/dev/files"
REMOTE_PIPELINE_FOLDER_NAME = "xio-gems/files"

def is_running_in_databricks():
    return "DATABRICKS_RUNTIME_VERSION" in os.environ

IS_DATABRICKS = is_running_in_databricks()
print(f">>> Running in Databricks:\n\t{IS_DATABRICKS}")

if IS_DATABRICKS:
    curr_dir = (
        "/Workspace"
        + dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )
else:
    curr_dir = os.path.abspath(__file__)
print(f">>> Current directory:\n\t{curr_dir}")

if curr_dir.startswith("/Workspace/XIO/DEV") or curr_dir.startswith("/Workspace/XIO/PROD"):
    IS_FEATURE = False
    IS_BUNDLE = True
else:
    IS_FEATURE = True
    if curr_dir.startswith("/Workspace/Users") and ".bundle" in curr_dir:
        IS_BUNDLE = True
    else:
        IS_BUNDLE = False

print(f">>> Is bundle:\n\t{IS_BUNDLE}")
print(f">>> Running from feature branch:\n\t{IS_FEATURE}")

if IS_FEATURE:
    if IS_BUNDLE:
        PIPELINE_FOLDER_NAME = FEAT_BUNDLE_PIPELINE_FOLDER_NAME
    else:
        PIPELINE_FOLDER_NAME = FEAT_NONBUNDLE_PIPELINE_FOLDER_NAME
else:
    PIPELINE_FOLDER_NAME = REMOTE_PIPELINE_FOLDER_NAME

SYS_BASE_PATH = curr_dir.split(f"/{PIPELINE_FOLDER_NAME}")[0]
print(f">>> Base path:\n\t{SYS_BASE_PATH}")

print(f">>> Importing pipeline config & models from:\n\t{SYS_BASE_PATH}/{PIPELINE_FOLDER_NAME}")
sys.path.append(f'{SYS_BASE_PATH}/{PIPELINE_FOLDER_NAME}/')
from pipeline_parameters import DEVELOPMENT_PREFIX, SILVER_DATABASE

if IS_FEATURE and not IS_DATABRICKS:
    FUNCTIONS_PATH = f"{SYS_BASE_PATH}/common/funcs"
else:
    if DEVELOPMENT_PREFIX == "production":
        FUNCTIONS_PATH = "/Workspace/XIO/PROD/xio-common-functions/files/funcs"
    else:
        FUNCTIONS_PATH = "/Workspace/XIO/DEV/xio-common-functions/files/funcs"

print(f">>> Importing funcs from:\n\t{FUNCTIONS_PATH}")

sys.path.append(FUNCTIONS_PATH)
from utils import *

# dbutils.widgets.text("tgt_table", "")
# dbutils.widgets.text("target_tbl_prefix", "")
# dbutils.widgets.text("bronze_tbl_prefix", "")
# dbutils.widgets.text("silver_tbl_prefix", "")
# dbutils.widgets.text("gold_tbl_prefix", "")
# dbutils.widgets.text("notebook_params", "")

tgt_table         = dbutils.widgets.get("tgt_table")
target_tbl_prefix = dbutils.widgets.get("target_tbl_prefix")
bronze_tbl_prefix = dbutils.widgets.get("bronze_tbl_prefix")
silver_tbl_prefix = dbutils.widgets.get("silver_tbl_prefix")
gold_tbl_prefix   = dbutils.widgets.get("gold_tbl_prefix")

notebook_params_raw = dbutils.widgets.get("notebook_params")
notebook_params     = json.loads(notebook_params_raw) if notebook_params_raw else {}

match_keys       = notebook_params.get("match_keys", [])
load_date_column = notebook_params.get("load_date_column", "refresh_timestamp")
save_mode        = notebook_params.get("save_mode")
period_column    = notebook_params.get("period_column", "")

print(tgt_table)
print(target_tbl_prefix)
print(bronze_tbl_prefix)
print(silver_tbl_prefix)
print(gold_tbl_prefix)
print(match_keys)
print(load_date_column)
print(save_mode)
print(period_column)

# --- 2. THE CORE GRAPH ENGINE ---
def calculate_relationships(df, max_level=15, min_pct=1e-6, max_paths_per_source=50000, all_results=False):
    """
    Compute time-versioned ownership relationships from dated edge events with targeted recomputation and pruning.

    This function ingests a table of directed ownership edges (parent → child) with start/end dates, constructs
    a chronological event stream, and incrementally updates a graph to enumerate effective ownership *paths*
    (parent → ... → child) affected on each transaction date. Enumeration is limited by depth, percentage
    threshold, and per-source path budget. It supports active/inactive relationships and ensures that only
    directly affected subgraphs are recalculated when changes occur.

    Key behaviours
    ---------------
    - **Edge closure handling:** `active_to_date` closes an edge by setting its ownership percentage to 0.0 from that date.
    - **Zero-start handling:** a "start at 0%" is treated as an explicit end event.
    - **Targeted recomputation:** when an edge changes, only its *ancestors* (upstream) and *descendants* (downstream)
      are recomputed — not all relationships under the same parent.
    - **0% edge pruning:** zero-percentage edges are dropped from traversal only if replaced by a positive-percentage
      start on the same date.
    - **Path enumeration:** performed via an iterative, pruned depth-first search:
        * maximum depth = `max_level`
        * minimum path percentage = `min_pct`
        * per-source cap = `max_paths_per_source`
    - **Relationship identification:** each enumerated path yields a `rel_id` built by joining edge IDs with “_”
      (e.g. `"A.B_B.C_C.D"`).
    - **Versioning:** within each `rel_id`, records are ordered oldest-first (`ascending=[True, True]`), so
      `relationship_version=1` corresponds to the *earliest* appearance of that state.
    - **Filtering:** by default, only rows where the ownership percentage differs from the previous version
      are retained. Set `all_results=True` to include the unfiltered results and full event stream.

    Expected columns in `df`
    ------------------------
    - `active_from_date` (required): start date of the edge (inclusive)
    - `active_to_date` (optional): end date of the edge (inclusive → becomes 0% on that date)
    - `parent_company_id` (required)
    - `company_id` (required)
    - `ownership_percentage` (required, float in [0, 1])

    Parameters
    ----------
    df : pandas.DataFrame
        Input edges with the columns listed above.
        If converting from Spark, ensure timestamps are formatted as strings before `.toPandas()`
        to avoid pandas OutOfBoundsDatetime errors.
    max_level : int, default 15
        Maximum traversal depth (edges) in the DFS.
    min_pct : float, default 1e-6
        Minimum ownership percentage threshold to continue or emit a path.
    max_paths_per_source : int, default 50000
        Maximum number of paths to yield per changed source per transaction date.
    all_results : bool, default False
        If True, returns a tuple `(results_df_masked, results_df_full, events)`:
          - `results_df_masked`: filtered to percentage changes only
          - `results_df_full`: full unfiltered results
          - `events`: processed event stream
        If False, returns `(results_df_masked, events)`.

    Returns
    -------
    pandas.DataFrame or tuple
        When `all_results=False`:
            (`results_df_masked`, `events`)
            where `results_df_masked` contains:
              - `rel_id` (str): path identifier (joined edge IDs with "_")
              - `percentage` (float): effective path percentage on `transaction_date`
              - `transaction_date` (datetime64[ns]): date evaluated
              - `relationship_version` (int): 1 = earliest per `rel_id`
              - `ultimate_child_id` (str): last node in path
              - `ultimate_parent_id` (str): first node in path
              - `relationship_level` (int): number of direct edges in the path
        When `all_results=True`:
            (`results_df_masked`, `results_df_full`, `events`)

    Notes
    -----
    - If multiple events occur for the same edge on the same date, the “start” (positive percentage)
      overrides the “end” (0%) on that date.
    - The algorithm guarantees simple paths (no cycles); cycles are skipped.
    - The function returns an empty, typed DataFrame when no relationships are produced.
    - Timestamps outside pandas' valid datetime64[ns] range (year > 2262) must be pre-converted
      to strings before calling this function.
    """
    def yield_paths_pruned(G, src, cutoff=6, min_pct=1e-6, max_paths=50000):
        stack = [(src, [src], 1.0)]
        yielded = 0
        while stack:
            u, path, pct = stack.pop()
            if len(path) - 1 >= cutoff:
                continue
            for v in G.successors(u):
                d = G[u][v]
                edge_pct = d.get("percentage", 0.0)
                new_pct = pct * edge_pct
                if new_pct <= 0.0 or new_pct < min_pct:
                    continue
                if v in path:
                    continue
                new_path = path + [v]
                yield new_path, new_pct
                yielded += 1
                if yielded >= max_paths:
                    return
                stack.append((v, new_path, new_pct))

    df = df.copy()
    df["active_from_date"] = pd.to_datetime(df["active_from_date"])
    if "active_to_date" in df.columns:
        df["active_to_date"] = pd.to_datetime(df["active_to_date"])
    else:
        df["active_to_date"] = pd.NaT

    df["rel_id"] = df["parent_company_id"].astype(str) + "." + df["company_id"].astype(str)

    start_events = (
        df[df["ownership_percentage"] > 0]
        .rename(columns={"active_from_date": "event_date"})
        .assign(_kind="start")[
            ["event_date", "rel_id", "parent_company_id", "company_id", "ownership_percentage", "transaction_type", "_kind"]
        ]
    )

    end_events = pd.concat([
        df[df["active_to_date"].notna()]
        .rename(columns={"active_to_date": "event_date"})
        .assign(ownership_percentage=0.0, _kind="end")[
            ["event_date", "rel_id", "parent_company_id", "company_id", "ownership_percentage", "transaction_type", "_kind"]
        ],
        df[(df["active_to_date"].isna()) & (df["ownership_percentage"] == 0)]
        .rename(columns={"active_from_date": "event_date"})
        .assign(_kind="end")[
            ["event_date", "rel_id", "parent_company_id", "company_id", "ownership_percentage", "transaction_type", "_kind"]
        ],
    ], ignore_index=True)

    events = (
        pd.concat([start_events, end_events], ignore_index=True)
        .assign(_prio=lambda x: x["_kind"].map({"end": 0, "start": 1}))
        .sort_values(["event_date", "rel_id", "_prio"])
        .drop_duplicates(subset=["event_date", "rel_id"], keep="last")
        .reset_index(drop=True)
    )

    G = nx.DiGraph()
    seen_records = pd.DataFrame(columns=events.columns)
    results_dict = {}
    edge_to_paths = {}  

    for date in sorted(events["event_date"].unique()):
        todays = events[events["event_date"] == date]
        seen_records = pd.concat([seen_records, todays], ignore_index=True)

        latest_by_rel = seen_records.sort_values(["rel_id", "event_date"]).drop_duplicates(subset=["rel_id"], keep="last")

        for _, row in latest_by_rel.iterrows():
            G.add_edge(
                row["parent_company_id"],
                row["company_id"],
                rel_id=row["rel_id"], 
                percentage=row["ownership_percentage"],
                transaction_type=row["transaction_type"]
            )

        starts_today = start_events[start_events["event_date"].eq(date)][["rel_id"]]
        ends_today = end_events[end_events["event_date"].eq(date)][["rel_id"]]
        superseded_rel_ids = set(starts_today["rel_id"]).intersection(ends_today["rel_id"])

        zero_ends_today = end_events[end_events["event_date"].eq(date) & ~end_events["rel_id"].isin(superseded_rel_ids)]
        for _, r in zero_ends_today.iterrows():
            key = (r["rel_id"], pd.Timestamp(date))
            tx_type = r["transaction_type"]

            results_dict[key] = {
                "rel_id": r["rel_id"],
                "percentage": 0.0, 
                "transaction_date": pd.Timestamp(date),
                "transaction_type": tx_type
            }
            for full_rel in edge_to_paths.get(r["rel_id"], ()):
                results_dict[(full_rel, pd.Timestamp(date))] = {
                    "rel_id": full_rel,
                    "percentage": 0.0,
                    "transaction_date": pd.Timestamp(date),
                    "transaction_type": tx_type
                }

        changed_edges = list(zip(todays["parent_company_id"], todays["company_id"]))
        affected_nodes = set()

        for parent, child in changed_edges:
            affected_nodes.add(parent)
            affected_nodes.add(child)
            try: affected_nodes |= nx.ancestors(G, parent)
            except nx.NetworkXError: pass
            try: affected_nodes |= nx.descendants(G, child)
            except nx.NetworkXError: pass

        if not affected_nodes:
            continue
        Gsub = G.subgraph(affected_nodes).copy()

        zero_edges_superseded = [
            (u, v) for u, v, d in Gsub.edges(data=True)
            if d.get("percentage", 0.0) == 0.0 and d.get("rel_id") in superseded_rel_ids
        ]
        if zero_edges_superseded:
            Gsub.remove_edges_from(zero_edges_superseded)

        for src in affected_nodes:
            if src not in Gsub:
                continue
            for path_nodes, path_pct in yield_paths_pruned(
                Gsub, src, cutoff=max_level, min_pct=min_pct, max_paths=max_paths_per_source,
            ):
                path_edges = list(zip(path_nodes[:-1], path_nodes[1:]))
                rel_ids = [Gsub[u][v]["rel_id"] for u, v in path_edges]
                if not rel_ids:
                    continue

                raw_tx_types = []
                for u, v in path_edges:
                    t = Gsub[u][v].get("transaction_type")
                    if pd.notna(t) and t != "":
                        # Split by comma in case an edge already has multiple types 
                        raw_tx_types.extend([x.strip() for x in str(t).split(",")])
                
                # Sort and remove duplicates
                unique_tx = sorted(list(set([t for t in raw_tx_types if t])))
                merged_tx_type = ", ".join(unique_tx) if unique_tx else None

                full_rel_id = "_".join(rel_ids)
                key = (full_rel_id, pd.Timestamp(date))
                results_dict[key] = {
                    "rel_id": full_rel_id,
                    "percentage": path_pct,
                    "transaction_date": pd.Timestamp(date),
                    "transaction_type": merged_tx_type
                }
                for base in rel_ids:
                    s = edge_to_paths.get(base)
                    if s is None:
                        s = edge_to_paths[base] = set()
                    s.add(full_rel_id)

    results_df = pd.DataFrame(results_dict.values())
    if results_df.empty:
        empty_df = results_df.assign(
            ultimate_child_id=pd.Series(dtype="object"), 
            ultimate_parent_id=pd.Series(dtype="object"), 
            relationship_level=pd.Series(dtype="int64"),
            transaction_type=pd.Series(dtype="object")
        )
        print("⚠️ Graph Engine evaluated to empty. Returning blank schema.")
        if not all_results: 
            return empty_df, events
        else: 
            return empty_df, empty_df, events

    results_df = results_df.sort_values(["rel_id", "transaction_date"], ascending=[True, True]).reset_index(drop=True)
    results_df["relationship_version"] = results_df.groupby("rel_id").cumcount() + 1
    results_df["ultimate_child_id"] = results_df["rel_id"].str.split(".").str[-1]
    results_df["ultimate_parent_id"] = results_df["rel_id"].str.split(".").str[0]
    results_df["relationship_level"] = results_df["rel_id"].str.count(r"\.")

    mask = results_df["percentage"].ne(results_df.groupby("rel_id")["percentage"].shift())
    results_df_masked = results_df[mask].reset_index(drop=True)

    if not all_results: return results_df_masked, events
    else: return results_df_masked, results_df, events

def sanitize_timestamps(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, TimestampType):
            df = df.withColumn(
                field.name,
                f.when(
                    (f.col(field.name) >= f.lit("1900-01-01 00:00:00")) & (f.col(field.name) <= f.lit("2260-01-01 00:00:00")),
                    f.col(field.name),
                ).otherwise(f.lit(None).cast("timestamp")),
            )
    return df

# --- 3. EXECUTION ---
src_table = "shareholdings"
print(f"Reading source edges from {silver_tbl_prefix}{src_table}...")
shareholdings = spark.table(f"{silver_tbl_prefix}{src_table}")

# Format to exactly what calculate_relationships expects
comp_ownr_df = (
    shareholdings
    .withColumnRenamed("company_core_id", "company_id")
    # Assuming your SQL model outputs percentage as 0-100, we convert it to 0-1 for the Python function
    .withColumn("ownership_percentage", (f.col("ownership_percentage") / 100).cast("double"))
    .orderBy("active_from_date")
)

comp_ownr_df = sanitize_timestamps(comp_ownr_df)

print(f"Processing {comp_ownr_df.count()} edge records through NetworkX...")
# Run the Graph Engine!
new_rels_pd, events = calculate_relationships(comp_ownr_df.toPandas())

# Convert back to Spark
output_table = (
    spark.createDataFrame(new_rels_pd)
    .withColumn("transaction_date", f.col("transaction_date").cast("date"))
    .withColumn("refresh_timestamp", f.current_timestamp())
    .select(
        "rel_id",
        "ultimate_parent_id",
        "ultimate_child_id",
        "percentage",
        "transaction_date",
        "transaction_type",
        "relationship_level",
        "relationship_version",
        "refresh_timestamp"
    )
)

print(f"Generated {output_table.count()} multi-tier relationship paths.")

# COMMAND ----------

# DBTITLE 1,Save to delta
print("Saving data to Delta...")

if save_mode == "append":
    append_to_delta(spark, output_table, f"{target_tbl_prefix}{tgt_table}")
elif save_mode == "overwrite":
    overwrite_to_delta(spark, output_table, f"{target_tbl_prefix}{tgt_table}")
elif save_mode == "merge":
    merge_into_delta(spark,
                     df=output_table,
                     target=f"{target_tbl_prefix}{tgt_table}",
                     match_keys=match_keys,
                     period_column=period_column)
else:
    raise Exception(f"Invalid save mode: {save_mode}")

# COMMAND ----------

