import uuid
import threading
import logging
import sys
from typing import Dict, List
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

from .translator import DQTranslator
from .config_loader import load_combined_config


class DQRunner:
    def __init__(self, spark: SparkSession, runtime: Dict, base_path: str):
        self.spark          = spark
        self.runtime        = runtime
        self.base_path      = base_path
        try:
            self.ws         = WorkspaceClient()
            self.dq_engine  = DQEngine(self.ws)
        except Exception as e:
            raise RuntimeError(f"Failed to initialize Workspace Client or DQEngine: {e}")
        self.translator     = DQTranslator(self.ws, base_path)
        self._ai_lock       = threading.Lock()                      # thread Lock for AI generation to prevent RateLimitErrors
        _                   = self.translator.generator             # warms up generator in main thread


    def run_for_table(self, config_rel_path: str, table_name: str):
        run_id = str(uuid.uuid4())

        try:
            full_cfg = load_combined_config(self.base_path, config_rel_path, table_name)
        except Exception as e:
            raise RuntimeError(f"Config Error for {table_name}: {e}")

        table_cfg = full_cfg["table"]
        source_fqtn = f"{self.runtime['source_db']}.{table_cfg['table_name']}"
        table_cfg["fqtn"] = source_fqtn

        # Translate Rules
        error_checks, warn_checks, all_check_names = [], [], []
        for check_item in full_cfg["merged_checks"]:
            try:
                if "ai_prompt" in check_item:
                    with self._ai_lock:         
                        translated_list = self.translator.translate(check_item, table_cfg, config_rel_path)
                else:
                    translated_list = self.translator.translate(check_item, table_cfg, config_rel_path)
                
                # put rule logic into criticality buckets
                for item in translated_list:
                    rule_logic = item['rule']
                    all_check_names.append(item['name'])
                    
                    if item['criticality'] == 'warn':
                        warn_checks.append(rule_logic)
                    else:
                        error_checks.append(rule_logic)
            except Exception as e:
                rule_desc = check_item.get('name') or check_item.get('template') or "Unknown Rule"
                raise RuntimeError(f"Rule Translation failed for {table_name} [{rule_desc}]: {e}")
        
        try:
            df = self.spark.read.table(source_fqtn)
            total_count = df.count()
        except Exception as e:
            raise RuntimeError(f"Data Access Error: Could not read table {source_fqtn}. Check permissions: {e}")
        
        try:
            valid_df, invalid_df = self.dq_engine.apply_checks_by_metadata_and_split(df, error_checks) 
            _, warning_df = self.dq_engine.apply_checks_by_metadata_and_split(df, warn_checks)         
        except Exception as e:
            raise RuntimeError(f"DQ Execution failed on Spark for table {table_name}: {e}")  
        
        return self._process_results(
            run_id, 
            table_cfg["table_name"], 
            invalid_df,   
            warning_df,   
            total_count, 
            len(error_checks) + len(warn_checks),
            table_cfg,
            all_check_names
        )


    def _process_results(self, run_id, table_name, invalid_df, warning_df, total_count, num_checks, table_cfg, check_names):
        dq_db = self.runtime['dq_db']
        pk_cols = table_cfg.get("roles", {}).get("primary_keys", [])

        try:
            # Add Criticality
            if invalid_df:
                invalid_df = invalid_df.withColumn("_check_criticality", F.lit("ERROR"))
            if warning_df:
                warning_df = warning_df.withColumn("_check_criticality", F.lit("WARN"))

            unified_df = None
            if invalid_df and warning_df:
                unified_df = invalid_df.unionByName(warning_df, allowMissingColumns=True)
            else:
                unified_df = invalid_df or warning_df

            issues_table_name = None
            if unified_df:
                # 2. Combine _errors and _warnings so we don't lose soft checks, then explode
                combined_issues = F.concat(
                    F.coalesce(F.col("_errors"), F.array()), 
                    F.coalesce(F.col("_warnings"), F.array())
                )
                exploded_df = unified_df.withColumn("failed_check", F.explode(combined_issues))

                # 3. Extract the metadata from the exploded JSON object
                final_issues_df = (
                    exploded_df
                    .withColumn("_check_name", F.col("failed_check.name"))
                    .withColumn("_check_function", F.col("failed_check.function"))
                    .withColumn("_check_message", F.col("failed_check.message"))
                    .withColumn("_check_column", 
                        F.coalesce(
                            F.col("failed_check.columns").cast("string"), 
                            F.col("failed_check.user_metadata.check_columns").cast("string")
                        )
                    )
                    .withColumn("_check_run_time", F.col("failed_check.run_time"))
                    .withColumn("_check_run_id", F.col("failed_check.run_id"))
                    .drop("failed_check", "_errors", "_warnings") 
                )

                issues_table_name = f"std__{table_name}"
                (final_issues_df.write.format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", "true") 
                    .saveAsTable(f"{dq_db}.{issues_table_name}"))
                
        except Exception as e:
            raise RuntimeError(f"Result Storage Error for {table_name}: Failed writing to {dq_db}. Error: {e}")
            

        def get_unique_row_count(df, pks):
            try:
                if df is None or df.isEmpty():
                    return 0
                
                valid_pks = [c for c in pks if c in df.columns]
                if valid_pks:
                    return df.select(valid_pks).distinct().count()
                return df.count()
            except Exception:
                return 0
        
        invalid_row_count = get_unique_row_count(invalid_df, pk_cols)
        warning_row_count = get_unique_row_count(warning_df, pk_cols)
        success_rate_float = 100.0
        if total_count > 0:
            success_rate_float = round(((total_count - invalid_row_count) / total_count) * 100, 2)

        return {
            "emoji": "❌" if invalid_row_count > 0 else ("⚠️" if warning_row_count > 0 else "✅"),
            "table_name": table_name,
            "check_names": check_names,
            "status": "Issues Found" if invalid_row_count > 0 else ("Warning" if warning_row_count > 0 else "Success"),
            "success_rate": float(success_rate_float),
            "failed_rows": invalid_row_count,
            "warning_rows": warning_row_count,
            "total_rows": total_count,
            "checks_run": num_checks,
            "issues_table": f"SELECT * FROM {dq_db}.{issues_table_name}",
            "run_id": run_id
        }