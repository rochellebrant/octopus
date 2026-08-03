import requests
from pyspark.sql.functions import col, from_unixtime, collect_set, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType


# ---------------------------------------------------------
# Authentication Helper (Refactored)
# ---------------------------------------------------------
def get_databricks_api_client(dbutils):
    """
    Extracts workspace URL and token for internal API calls.
    Note: dbutils must be passed in from the executing Databricks environment.
    """
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    return {
        "host": ctx.apiUrl().get(),
        "headers": {"Authorization": f"Bearer {ctx.apiToken().get()}"},
    }


# ---------------------------------------------------------
# Workspace Jobs Fetcher
# ---------------------------------------------------------
def fetch_all_workspace_jobs(host, headers):
    """Fetches every job in the workspace, handling pagination."""
    all_jobs = []
    has_more = True
    page_token = None

    while has_more:
        params = {"expand_tasks": "false"}
        if page_token:
            params["page_token"] = page_token

        resp = requests.get(
            f"{host}/api/2.1/jobs/list", headers=headers, params=params
        ).json()
        all_jobs.extend(resp.get("jobs", []))

        has_more = resp.get("has_more", False)
        page_token = resp.get("next_page_token")

    return all_jobs


# ---------------------------------------------------------
# Tag Filter Utility
# ---------------------------------------------------------
def filter_jobs_by_tags(jobs_list, required_tags):
    """
    Filters jobs containing ALL tags specified in required_tags.
    Matches against both keys and values in the job's tag dictionary.
    """
    tagged_jobs = []
    # Ensure our required tags are lowercase for safe comparison
    required_set = set([str(t).lower() for t in required_tags])

    for j in jobs_list:
        tags_dict = j.get("settings", {}).get("tags", {})

        # Lowercase all keys and values from the job
        job_tags = set(
            [str(k).lower() for k in tags_dict.keys()]
            + [str(v).lower() for v in tags_dict.values()]
        )

        # issubset checks if all required_tags exist in the job_tags
        if required_set.issubset(job_tags):
            tagged_jobs.append(j)

    return tagged_jobs


# ---------------------------------------------------------
# Job Runs Fetcher
# ---------------------------------------------------------
def fetch_latest_run_statuses(jobs_list, host, headers):
    """Fetches the most recent run state and the last successful run time."""
    data = []
    for j in jobs_list:
        # Increase limit to 25 to give us a buffer to find a recent success
        run_resp = requests.get(
            f"{host}/api/2.1/jobs/runs/list",
            headers=headers,
            params={"job_id": j["job_id"], "limit": 25},
        ).json()

        runs = run_resp.get("runs", [])
        latest = runs[0] if runs else {}

        # Scan the list of runs to find the most recent one that succeeded
        last_success_time = None
        for run in runs:
            if run.get("state", {}).get("result_state") == "SUCCESS":
                last_success_time = run.get("start_time")
                break  # Stop at the first success we find

        data.append(
            {
                "job_id": str(j["job_id"]),
                "job_name": j["settings"]["name"],
                "status": latest.get("state", {}).get("result_state", "UNKNOWN"),
                "life_cycle": latest.get("state", {}).get("life_cycle_state", "N/A"),
                "start_time": latest.get("start_time"),
                "last_success_start_time": last_success_time,  # New field captured
                "run_url": latest.get("run_page_url", ""),
            }
        )
    return data


# ---------------------------------------------------------
# Unity Catalog Lineage Fetcher
# ---------------------------------------------------------
def get_job_table_lineage(spark, days_back=30):
    """Queries Unity Catalog system tables for job-to-table lineage."""
    query = f"""
        SELECT DISTINCT
            entity_id AS job_id,
            target_table_full_name
        FROM system.access.table_lineage
        WHERE entity_type = 'JOB'
          AND event_time >= current_date() - INTERVAL {days_back} DAYS
          AND target_table_full_name IS NOT NULL
    """
    return spark.sql(query)


# ---------------------------------------------------------
# Pipeline Health Table Orchestrator
# ---------------------------------------------------------
def update_pipeline_health_table(
    spark, dbutils, required_tags: list, catalog: str, schema: str, table: str
):
    """
    Orchestrates the fetching, joining, and saving of pipeline health data.
    """
    client = get_databricks_api_client(dbutils)
    print(">>> Fetching all workspace jobs.")

    all_jobs = fetch_all_workspace_jobs(client["host"], client["headers"])
    print(f">>> Found {len(all_jobs)} total jobs.")
    my_team_jobs = filter_jobs_by_tags(all_jobs, required_tags)
    print(f">>> Found {len(my_team_jobs)} target jobs with tags {required_tags}.")

    if not my_team_jobs:
        print("No matching jobs found. Exiting gracefully.")
        return None

    raw_data = fetch_latest_run_statuses(
        my_team_jobs, client["host"], client["headers"]
    )

    schema_def = StructType(
        [
            StructField("job_id", StringType(), True),
            StructField("job_name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("life_cycle", StringType(), True),
            StructField("start_time", LongType(), True),
            StructField("last_success_start_time", LongType(), True),
            StructField("run_url", StringType(), True),
        ]
    )

    status_df = spark.createDataFrame(raw_data, schema=schema_def)
    lineage_df = get_job_table_lineage(spark, days_back=30)

    # Use implicit line continuation to keep lines under 99 characters
    final_df = (
        status_df.alias("s")
        .join(lineage_df.alias("l"), on="job_id", how="left")
        .groupBy(
            "job_id",
            "job_name",
            "status",
            "life_cycle",
            "start_time",
            "last_success_start_time",
            "run_url",
        )
        .agg(collect_set("target_table_full_name").alias("associated_tables"))
        .withColumn("last_run_at", from_unixtime(col("start_time") / 1000))
        .withColumn(
            "last_successful_run_at",
            from_unixtime(col("last_success_start_time") / 1000),
        )
        .withColumn("health_data_refreshed_at", current_timestamp())
        .drop("start_time", "last_success_start_time")
    )

    full_table_name = f"{catalog}.{schema}.{table}"
    final_df.write.mode("overwrite").saveAsTable(full_table_name)
    print(f"✅ Successfully updated health table: {full_table_name}")

    return final_df
