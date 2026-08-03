from pyspark.sql import functions as F, Window as W
from pyspark.sql import DataFrame


class TemporalToolkit:
    """
    A unified engine for Temporal Normalization (Time-Slicing).
    Designed to support both internal table slicing and multi-source timeline anchoring.
    """

    FAR_FUTURE = "9999-12-31 23:59:59"

    @staticmethod
    def standardize_intervals(
        df: DataFrame, start_col: str, end_col: str, null_replacement: str = FAR_FUTURE
    ) -> DataFrame:
        """Ensures columns are timestamps and fills open-ended dates."""
        return df.withColumns(
            {
                start_col: F.to_timestamp(F.col(start_col)),
                end_col: F.coalesce(
                    F.to_timestamp(F.col(end_col)),
                    F.to_timestamp(F.lit(null_replacement)),
                ),
            }
        )

    @staticmethod
    def get_segments_from_single_table(
        df: DataFrame, key_cols: list, timestamp_cols: list
    ) -> DataFrame:
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
    def get_segments_from_multisource(
        anchor_df: DataFrame, key_cols: list, source_configs: list
    ) -> tuple[DataFrame, DataFrame]:
        combined_bounds = None

        for config in source_configs:
            src_df, s_col, e_col, src_name = config[0], config[1], config[2], config[3]
            rename_dict = config[4] if len(config) > 4 else {}

            for old_name, new_name in rename_dict.items():
                src_df = src_df.withColumnRenamed(old_name, new_name)

            # 1. Identify keys shared between this specific source and the anchor
            available_keys = [k for k in key_cols if k in src_df.columns]

            # 2. Get raw boundaries from the source
            b_from = src_df.select(
                *available_keys,
                F.col(s_col).alias("boundary"),
                F.lit(f"{src_name}_from").alias("source"),
            )
            b_to = src_df.select(
                *available_keys,
                F.col(e_col).alias("boundary"),
                F.lit(f"{src_name}_to").alias("source"),
            )
            batch = b_from.unionByName(b_to).where("boundary is not null")

            # 3. Join to anchor_df to fan out the dates to the full anchor grain
            fanned_batch = anchor_df.join(batch, on=available_keys, how="inner").select(
                *key_cols, "boundary", "source"
            )

            combined_bounds = (
                fanned_batch
                if combined_bounds is None
                else combined_bounds.unionByName(fanned_batch)
            )

        # --- THE CRITICAL FIX ---
        # We must ensure we are only sliding the window over UNIQUE boundaries per anchor combo
        unique_bounds = combined_bounds.select(*key_cols, "boundary").distinct()

        # 4. Create provenance summary (opened_by_sources)
        summary = combined_bounds.groupBy(*key_cols, "boundary").agg(
            F.sort_array(F.collect_set("source")).alias("opened_by_sources")
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
    def join_state_to_segments(
        segments_df: DataFrame,
        windows_df: DataFrame,
        key_cols: list,
        seg_start: str,
        seg_end: str,
        win_start: str,
        win_end: str,
        join_type: str = "left",
        broadcast_right: bool = False,
    ) -> DataFrame:
        """The core temporal join: ensures segment is fully covered by state window."""
        s, w = segments_df.alias("seg"), windows_df.alias("win")

        join_cond = [s[c] == w[c] for c in key_cols] + [
            s[seg_start] >= w[win_start],
            s[seg_end] <= w[win_end],
        ]

        right_side = F.broadcast(w) if broadcast_right else w
        joined = s.join(right_side, join_cond, join_type)

        for c in key_cols:
            joined = joined.drop(w[c])
        return joined

    @staticmethod
    def build_boundaries_from_join(
        anchor_df: DataFrame,
        join_df: DataFrame,
        join_cond,
        from_col: str,
        to_col: str,
        source_name: str,
        anchor_keys: list,
        join_type: str = "left",
    ) -> DataFrame:
        """
        Joins anchor_df to join_df on an arbitrary condition and emits one boundary event per
        anchor key for each non-null from_col/to_col value found, tagged with a "<source_name>_from"
        / "<source_name>_to" source label. Used to build a multi-source boundary catalog ahead of
        slicing a timeline into minimal segments (see collapse_segments for the merge step).

        Unlike get_segments_from_multisource, this accepts any join_cond (not just equality on a
        shared key list), so it supports joins with mismatched column names, multi-source-specific
        prefixes, or extra predicates alongside key equality.
        """
        j = anchor_df.alias("a").join(join_df.alias("d"), join_cond, join_type)

        b_from = (
            j.select(
                *[F.col(f"a.{c}").alias(c) for c in anchor_keys],
                F.col(from_col).cast("timestamp").alias("boundary"),
            )
            .where("boundary is not null")
            .withColumn("source", F.lit(source_name + "_from"))
        )
        b_to = (
            j.select(
                *[F.col(f"a.{c}").alias(c) for c in anchor_keys],
                F.col(to_col).cast("timestamp").alias("boundary"),
            )
            .where("boundary is not null")
            .withColumn("source", F.lit(source_name + "_to"))
        )
        return b_from.unionByName(b_to).dropDuplicates(
            anchor_keys + ["boundary", "source"]
        )

    @staticmethod
    def dedupe_segment_rows(
        df: DataFrame,
        key_cols: list,
        unit_cols: list = None,
        flag_specs: dict = None,
        pass_through_cols: list = None,
    ) -> DataFrame:
        """
        Deterministically collapse multiple candidate rows sharing the same key_cols down to
        exactly one row per key, via a single hash aggregation (no window sort, no join).

        Replaces the groupBy(...).agg(first(col, ignorenulls=True)) pattern, which picks
        arbitrarily (and can differ between runs) whenever more than one candidate row exists
        for a key -- e.g. two overlapping source windows both covering the same segment. A window
        with ORDER BY plus a join to merge in the flags achieves the same determinism but forces
        a sort-based shuffle per stage; min_by/max_by pick the winner within one hash aggregation.

        Picks the row with the fewest NULLs among unit_cols, tie-broken by a hash of the other
        columns so the same input always produces the same output regardless of Spark's internal
        row ordering.

        unit_cols: columns selected atomically from one winning row (avoids ever mixing fields
                   from different candidate rows, e.g. a milestone's id/type/phase together).
        flag_specs: dict of {output_alias: source_boolean_col}, OR'd (any-match semantics) across
                    ALL candidate rows for the key, not just the one row that wins the pick.
        pass_through_cols: columns taken from the winning row (values are already identical
                            across all candidate rows for a key, e.g. columns from an
                            already-deduped upstream stage).
        """
        unit_cols = unit_cols or []
        flag_specs = flag_specs or {}
        pass_through_cols = pass_through_cols or []

        tiebreak_cols = [c for c in df.columns if c not in key_cols]
        null_rank = (
            sum(F.when(F.col(c).isNull(), 1).otherwise(0) for c in unit_cols)
            if unit_cols
            else F.lit(0)
        )
        priority = F.struct(
            null_rank.alias("_nulls"), F.hash(*tiebreak_cols).alias("_tiebreak")
        )

        agg_exprs = [
            F.min_by(F.col(c), priority).alias(c)
            for c in (unit_cols + pass_through_cols)
        ]
        agg_exprs += [
            F.max(F.col(src)).alias(alias) for alias, src in flag_specs.items()
        ]

        return df.groupBy(*key_cols).agg(*agg_exprs)

    @staticmethod
    def collapse_segments(
        df: DataFrame,
        partition_cols: list,
        state_cols: list,
        start_col: str,
        end_col: str,
    ) -> DataFrame:
        """Gaps and Islands logic to merge adjacent rows with identical states."""
        w = W.partitionBy(*partition_cols).orderBy(start_col)

        same_state = F.lit(True)
        for c in state_cols:
            same_state = same_state & (F.col(c).eqNullSafe(F.lag(c).over(w)))

        new_group = (
            F.lag(end_col).over(w).isNull()
            | (F.lag(end_col).over(w) != F.col(start_col))
            | (~same_state)
        )

        df_grouped = df.withColumn(
            "_new_grp", F.when(new_group, 1).otherwise(0)
        ).withColumn("_grp_id", F.sum("_new_grp").over(w))

        agg_exprs = [F.min(start_col).alias(start_col), F.max(end_col).alias(end_col)]
        agg_exprs += [F.first(c, True).alias(c) for c in state_cols]

        if "opened_by_sources" in df.columns:
            agg_exprs.append(
                F.sort_array(
                    F.array_distinct(F.flatten(F.collect_list("opened_by_sources")))
                ).alias("opened_by_sources")
            )

        return (
            df_grouped.groupBy(*partition_cols, "_grp_id")
            .agg(*agg_exprs)
            .drop("_grp_id")
        )
