from pyspark.sql import functions as F, Window as W
from pyspark.sql import DataFrame

class TemporalToolkit:
    """
    A unified engine for Temporal Normalization (Time-Slicing).
    Designed to support both internal table slicing and multi-source timeline anchoring.
    """
    FAR_FUTURE = "9999-12-31 23:59:59"

    @staticmethod
    def standardize_intervals(df: DataFrame, start_col: str, end_col: str, 
                             null_replacement: str = FAR_FUTURE) -> DataFrame:
        """Ensures columns are timestamps and fills open-ended dates."""
        return df.withColumns({
            start_col: F.to_timestamp(F.col(start_col)),
            end_col: F.coalesce(F.to_timestamp(F.col(end_col)), F.to_timestamp(F.lit(null_replacement)))
        })

    @staticmethod
    def get_segments_from_single_table(df: DataFrame, key_cols: list, timestamp_cols: list) -> DataFrame:
        """
        PIECE 1 LOGIC: Creates segments by stacking columns from a single DataFrame.
        Used when a table needs to be sliced by its own internal dates.
        """
        n = len(timestamp_cols)
        stack_expr = f"stack({n}, {', '.join(timestamp_cols)}) as boundary"
        
        bounds = (
            df.select(*key_cols, F.expr(stack_expr))
            .where(F.col("boundary").isNotNull())
            .dropDuplicates(key_cols + ["boundary"])
        )
        return TemporalToolkit._build_slices(bounds, key_cols)

    # @staticmethod
    # def get_segments_from_multisource(anchor_df: DataFrame, key_cols: list, source_configs: list) -> tuple[DataFrame, DataFrame]:
    #     combined_bounds = None
        
    #     for config in source_configs:
    #         src_df, s_col, e_col, src_name = config[0], config[1], config[2], config[3]
    #         rename_dict = config[4] if len(config) > 4 else {}

    #         # 1. Apply renames
    #         for old_name, new_name in rename_dict.items():
    #             src_df = src_df.withColumnRenamed(old_name, new_name)
            
    #         # 2. SMART KEY SELECTION
    #         # Only use keys that exist in the Source DF
    #         available_keys = [k for k in key_cols if k in src_df.columns]
            
    #         # 3. Extract boundaries at the source grain (e.g., just asset_id)
    #         # We don't join to anchor_df yet!
    #         b_from = src_df.select(*available_keys, F.col(s_col).alias("boundary"), F.lit(f"{src_name}_from").alias("source"))
    #         b_to = src_df.select(*available_keys, F.col(e_col).alias("boundary"), F.lit(f"{src_name}_to").alias("source"))
            
    #         batch = b_from.unionByName(b_to).where("boundary is not null")
            
    #         # 4. Now join the raw boundaries to the anchor_df to "fan out" to the full grain
    #         fanned_batch = anchor_df.alias("a").join(batch.alias("b"), on=available_keys, how="inner") \
    #                                .select(*key_cols, "boundary", "source")

    #         combined_bounds = fanned_batch if combined_bounds is None else combined_bounds.unionByName(fanned_batch)

    #     # 5. We must deduplicate BEFORE we do the slice lead function.
    #     # This prevents "zero-length" segments where two sources change at the same time.
    #     unique_anchor_bounds = combined_bounds.dropDuplicates(key_cols + ["boundary"])

    #     # 6. Summary needs the 'source' labels, so we use the full combined_bounds here
    #     summary = (
    #         combined_bounds.groupBy(*key_cols, "boundary")
    #         .agg(F.sort_array(F.collect_set("source")).alias("opened_by_sources"))
    #     )
        
    #     # 6. Build segments using ONLY the unique points per anchor grain
    #     segments = TemporalToolkit._build_slices(unique_anchor_bounds, key_cols)
        
    #     return summary, segments

    @staticmethod
    def get_segments_from_multisource(anchor_df: DataFrame, key_cols: list, source_configs: list) -> tuple[DataFrame, DataFrame]:
        combined_bounds = None
        
        for config in source_configs:
            src_df, s_col, e_col, src_name = config[0], config[1], config[2], config[3]
            rename_dict = config[4] if len(config) > 4 else {}

            for old_name, new_name in rename_dict.items():
                src_df = src_df.withColumnRenamed(old_name, new_name)
            
            # 1. Identify keys shared between this specific source and the anchor
            available_keys = [k for k in key_cols if k in src_df.columns]
            
            # 2. Get raw boundaries from the source
            b_from = src_df.select(*available_keys, F.col(s_col).alias("boundary"), F.lit(f"{src_name}_from").alias("source"))
            b_to = src_df.select(*available_keys, F.col(e_col).alias("boundary"), F.lit(f"{src_name}_to").alias("source"))
            batch = b_from.unionByName(b_to).where("boundary is not null")
            
            # 3. Join to anchor_df to fan out the dates to the full anchor grain
            fanned_batch = anchor_df.join(batch, on=available_keys, how="inner") \
                                   .select(*key_cols, "boundary", "source")

            combined_bounds = fanned_batch if combined_bounds is None else combined_bounds.unionByName(fanned_batch)
        
        # --- THE CRITICAL FIX ---
        # We must ensure we are only sliding the window over UNIQUE boundaries per anchor combo
        unique_bounds = combined_bounds.select(*key_cols, "boundary").distinct()

        # 4. Create provenance summary (opened_by_sources)
        summary = (
            combined_bounds.groupBy(*key_cols, "boundary")
            .agg(F.sort_array(F.collect_set("source")).alias("opened_by_sources"))
        )
        
        # 5. Build segments - Using the distinct bounds to prevent the 900k explosion
        segments = TemporalToolkit._build_slices(unique_bounds, key_cols)
        
        return summary, segments
    

    @staticmethod
    def _build_slices(bounds_df: DataFrame, key_cols: list) -> DataFrame:
        """Internal helper to turn discrete boundaries into [start, end) intervals."""
        spec = W.partitionBy(*key_cols).orderBy("boundary")
        return (
            bounds_df.withColumn("seg_start", F.col("boundary"))
            .withColumn("seg_end", F.lead("boundary").over(spec))
            .where(F.col("seg_end").isNotNull())
            .where(F.col("seg_end") > F.col("seg_start"))
            .select(*key_cols, "seg_start", "seg_end")
        )

    @staticmethod
    def join_state_to_segments(segments_df: DataFrame, windows_df: DataFrame, 
                               key_cols: list, seg_start: str, seg_end: str, 
                               win_start: str, win_end: str, join_type: str = "left",
                               broadcast_right: bool = False) -> DataFrame:
        """The core temporal join: ensures segment is fully covered by state window."""
        s, w = segments_df.alias("seg"), windows_df.alias("win")
        
        join_cond = [s[c] == w[c] for c in key_cols] + [
            s[seg_start] >= w[win_start],
            s[seg_end] <= w[win_end]
        ]
        
        right_side = F.broadcast(w) if broadcast_right else w
        joined = s.join(right_side, join_cond, join_type)
        
        for c in key_cols:
            joined = joined.drop(w[c])
        return joined

    @staticmethod
    def collapse_segments(df: DataFrame, partition_cols: list, state_cols: list, 
                          start_col: str, end_col: str) -> DataFrame:
        """Gaps and Islands logic to merge adjacent rows with identical states."""
        w = W.partitionBy(*partition_cols).orderBy(start_col)
        
        same_state = F.lit(True)
        for c in state_cols:
            same_state = same_state & (F.col(c).eqNullSafe(F.lag(c).over(w)))

        new_group = (F.lag(end_col).over(w).isNull() | 
                     (F.lag(end_col).over(w) != F.col(start_col)) | 
                     (~same_state))
        
        df_grouped = (
            df.withColumn("_new_grp", F.when(new_group, 1).otherwise(0))
            .withColumn("_grp_id", F.sum("_new_grp").over(w))
        )

        agg_exprs = [F.min(start_col).alias(start_col), F.max(end_col).alias(end_col)]
        agg_exprs += [F.first(c, True).alias(c) for c in state_cols]
        
        if "opened_by_sources" in df.columns:
            agg_exprs.append(F.sort_array(F.array_distinct(F.flatten(F.collect_list("opened_by_sources")))).alias("opened_by_sources"))

        return df_grouped.groupBy(*partition_cols, "_grp_id").agg(*agg_exprs).drop("_grp_id")