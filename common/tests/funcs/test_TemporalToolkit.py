import sys, os

# Set up repo path
cwd = os.getcwd()
repo_root = os.path.abspath(os.path.join(cwd, ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from funcs.TemporalToolkit import TemporalToolkit

spark = SparkSession.builder.getOrCreate()


def _rows(df, cols):
    return sorted(tuple(r) for r in df.select(*cols).collect())


# -------------------------------------
# standardize_intervals
# -------------------------------------
def test_standardize_intervals_fills_open_ended_null():
    df = spark.createDataFrame(
        [("A", "2020-01-01", None)], "id string, start string, end string"
    )
    result = TemporalToolkit.standardize_intervals(df, "start", "end")
    row = result.collect()[0]
    assert row["end"].strftime("%Y-%m-%d %H:%M:%S") == "9999-12-31 23:59:59"


def test_standardize_intervals_custom_null_replacement():
    df = spark.createDataFrame(
        [("A", "2020-01-01", None)], "id string, start string, end string"
    )
    result = TemporalToolkit.standardize_intervals(
        df, "start", "end", null_replacement="2099-01-01 00:00:00"
    )
    row = result.collect()[0]
    assert row["end"].strftime("%Y-%m-%d %H:%M:%S") == "2099-01-01 00:00:00"


def test_standardize_intervals_casts_existing_values():
    df = spark.createDataFrame(
        [("A", "2020-01-01", "2020-06-01")], ["id", "start", "end"]
    )
    result = TemporalToolkit.standardize_intervals(df, "start", "end")
    row = result.collect()[0]
    assert row["start"].strftime("%Y-%m-%d") == "2020-01-01"
    assert row["end"].strftime("%Y-%m-%d") == "2020-06-01"


# -------------------------------------
# get_segments_from_single_table
# -------------------------------------
def test_get_segments_from_single_table_basic_slice():
    df = (
        spark.createDataFrame(
            [("A", "2020-01-01", "2020-06-01")],
            ["asset_id", "from_date", "to_date"],
        )
        .withColumn("from_date", F.to_timestamp("from_date"))
        .withColumn("to_date", F.to_timestamp("to_date"))
    )
    segments = TemporalToolkit.get_segments_from_single_table(
        df, ["asset_id"], ["from_date", "to_date"]
    )
    rows = _rows(segments, ["asset_id", "seg_start", "seg_end"])
    assert len(rows) == 1
    assert rows[0][0] == "A"


def test_get_segments_from_single_table_multiple_boundaries_produce_multiple_segments():
    df = spark.createDataFrame(
        [("A", "2020-01-01", "2020-06-01", "2020-03-01")],
        ["asset_id", "from_date", "to_date", "mid_date"],
    )
    for c in ("from_date", "to_date", "mid_date"):
        df = df.withColumn(c, F.to_timestamp(c))
    segments = TemporalToolkit.get_segments_from_single_table(
        df, ["asset_id"], ["from_date", "mid_date", "to_date"]
    )
    assert segments.count() == 2


def test_get_segments_from_single_table_dedupes_identical_boundaries():
    df = spark.createDataFrame(
        [("A", "2020-01-01", "2020-01-01", "2020-06-01")],
        ["asset_id", "from_date", "dup_date", "to_date"],
    )
    for c in ("from_date", "dup_date", "to_date"):
        df = df.withColumn(c, F.to_timestamp(c))
    segments = TemporalToolkit.get_segments_from_single_table(
        df, ["asset_id"], ["from_date", "dup_date", "to_date"]
    )
    # from_date == dup_date, so only one real boundary pair -> one segment, not a zero-length extra
    assert segments.count() == 1


# -------------------------------------
# join_state_to_segments
# -------------------------------------
def test_join_state_to_segments_matches_covering_window():
    segments = spark.createDataFrame(
        [("A", "2020-01-01", "2020-03-01")], ["asset_id", "seg_start", "seg_end"]
    )
    windows = spark.createDataFrame(
        [("A", "2020-01-01", "2020-06-01", "STATE_X")],
        ["asset_id", "win_start", "win_end", "state"],
    )
    for c in ("seg_start", "seg_end"):
        segments = segments.withColumn(c, F.to_timestamp(c))
    for c in ("win_start", "win_end"):
        windows = windows.withColumn(c, F.to_timestamp(c))

    joined = TemporalToolkit.join_state_to_segments(
        segments, windows, ["asset_id"], "seg_start", "seg_end", "win_start", "win_end"
    )
    row = joined.collect()[0]
    assert row["state"] == "STATE_X"


def test_join_state_to_segments_left_join_keeps_uncovered_segment():
    segments = spark.createDataFrame(
        [("A", "2020-01-01", "2020-03-01")], ["asset_id", "seg_start", "seg_end"]
    )
    windows = spark.createDataFrame(
        [("A", "2021-01-01", "2021-06-01", "STATE_X")],
        ["asset_id", "win_start", "win_end", "state"],
    )
    for c in ("seg_start", "seg_end"):
        segments = segments.withColumn(c, F.to_timestamp(c))
    for c in ("win_start", "win_end"):
        windows = windows.withColumn(c, F.to_timestamp(c))

    joined = TemporalToolkit.join_state_to_segments(
        segments,
        windows,
        ["asset_id"],
        "seg_start",
        "seg_end",
        "win_start",
        "win_end",
        join_type="left",
    )
    assert joined.count() == 1
    assert joined.collect()[0]["state"] is None


# -------------------------------------
# build_boundaries_from_join
# -------------------------------------
def test_build_boundaries_from_join_emits_from_and_to_events():
    anchor = spark.createDataFrame([("A1", "F1")], ["asset_id", "fund_core_id"])
    joinable = spark.createDataFrame(
        [("A1", "F1", "2020-01-01", "2021-01-01")],
        ["asset_id", "fund_core_id", "rel_from", "rel_to"],
    )
    cond = (F.col("a.asset_id") == F.col("d.asset_id")) & (
        F.col("a.fund_core_id") == F.col("d.fund_core_id")
    )

    result = TemporalToolkit.build_boundaries_from_join(
        anchor,
        joinable,
        cond,
        "d.rel_from",
        "d.rel_to",
        "rels",
        ["asset_id", "fund_core_id"],
    )
    rows = _rows(result, ["asset_id", "fund_core_id", "source"])
    assert rows == [
        ("A1", "F1", "rels_from"),
        ("A1", "F1", "rels_to"),
    ]


def test_build_boundaries_from_join_skips_null_boundaries():
    anchor = spark.createDataFrame([("A1", "F1")], ["asset_id", "fund_core_id"])
    joinable = spark.createDataFrame(
        [("A1", "F1", "2020-01-01", None)],
        "asset_id string, fund_core_id string, rel_from string, rel_to string",
    )
    cond = (F.col("a.asset_id") == F.col("d.asset_id")) & (
        F.col("a.fund_core_id") == F.col("d.fund_core_id")
    )

    result = TemporalToolkit.build_boundaries_from_join(
        anchor,
        joinable,
        cond,
        "d.rel_from",
        "d.rel_to",
        "rels",
        ["asset_id", "fund_core_id"],
    )
    rows = _rows(result, ["source"])
    assert rows == [("rels_from",)]


def test_build_boundaries_from_join_supports_non_equi_join_condition():
    # join_cond deliberately uses a range predicate rather than plain key equality,
    # which get_segments_from_multisource cannot express.
    anchor = spark.createDataFrame(
        [("A1", "2020-06-01")], ["asset_id", "as_of"]
    ).withColumn("as_of", F.to_timestamp("as_of"))
    joinable = spark.createDataFrame(
        [("A1", "2020-01-01", "2021-01-01"), ("A1", "2022-01-01", "2023-01-01")],
        ["asset_id", "rel_from", "rel_to"],
    )
    for c in ("rel_from", "rel_to"):
        joinable = joinable.withColumn(c, F.to_timestamp(c))

    cond = (
        (F.col("a.asset_id") == F.col("d.asset_id"))
        & (F.col("a.as_of") >= F.col("d.rel_from"))
        & (F.col("a.as_of") <= F.col("d.rel_to"))
    )

    result = TemporalToolkit.build_boundaries_from_join(
        anchor, joinable, cond, "d.rel_from", "d.rel_to", "rels", ["asset_id"]
    )
    # only the first joinable row (2020-01-01/2021-01-01) covers as_of=2020-06-01
    assert result.count() == 2


# -------------------------------------
# dedupe_segment_rows
# -------------------------------------
def test_dedupe_segment_rows_single_candidate_passes_through():
    df = spark.createDataFrame(
        [("X", "2020-01-01", "2020-02-01", 0.5, True)],
        ["asset_id", "start", "end", "ownership", "hit_rel"],
    )
    result = TemporalToolkit.dedupe_segment_rows(
        df,
        ["asset_id", "start", "end"],
        unit_cols=["ownership"],
        flag_specs={"has_relpath": "hit_rel"},
    )
    row = result.collect()[0]
    assert row["ownership"] == 0.5
    assert row["has_relpath"] is True


def test_dedupe_segment_rows_picks_fewer_nulls_among_unit_cols():
    # Two candidates for the same key: one is missing BOTH unit_cols, the other has both
    # populated. The row with fewer NULLs (zero) should win outright.
    df = spark.createDataFrame(
        [
            ("X", "2020-01-01", "2020-02-01", None, None),
            ("X", "2020-01-01", "2020-02-01", 0.7, "pairs_A"),
        ],
        ["asset_id", "start", "end", "ownership", "pairs_with_ownership"],
    )
    result = TemporalToolkit.dedupe_segment_rows(
        df,
        ["asset_id", "start", "end"],
        unit_cols=["ownership", "pairs_with_ownership"],
    )
    assert result.count() == 1
    row = result.collect()[0]
    assert row["ownership"] == 0.7
    assert row["pairs_with_ownership"] == "pairs_A"


def test_dedupe_segment_rows_unit_cols_picked_atomically_from_same_row():
    # Candidate A has (milestone_id, milestone_type) fully populated; candidate B has neither.
    # The result must be A's pair together -- never a mix of A's milestone_id with B's type.
    df = spark.createDataFrame(
        [
            ("X", "2020-01-01", "2020-02-01", "MILE_A", "TYPE_A"),
            ("X", "2020-01-01", "2020-02-01", None, None),
        ],
        ["asset_id", "start", "end", "milestone_id", "milestone_type"],
    )
    result = TemporalToolkit.dedupe_segment_rows(
        df, ["asset_id", "start", "end"], unit_cols=["milestone_id", "milestone_type"]
    )
    row = result.collect()[0]
    assert (row["milestone_id"], row["milestone_type"]) == ("MILE_A", "TYPE_A")


def test_dedupe_segment_rows_flags_are_ored_across_all_candidates():
    # Neither candidate row individually has both flags true, but the OR across all
    # candidates for the key should still surface both as True.
    df = spark.createDataFrame(
        [
            ("X", "2020-01-01", "2020-02-01", True, False),
            ("X", "2020-01-01", "2020-02-01", False, True),
        ],
        ["asset_id", "start", "end", "hit_a", "hit_b"],
    )
    result = TemporalToolkit.dedupe_segment_rows(
        df,
        ["asset_id", "start", "end"],
        flag_specs={"has_a": "hit_a", "has_b": "hit_b"},
    )
    row = result.collect()[0]
    assert row["has_a"] is True
    assert row["has_b"] is True


def test_dedupe_segment_rows_pass_through_cols_carried_from_winning_row():
    df = spark.createDataFrame(
        [
            ("X", "2020-01-01", "2020-02-01", None, "carried_value"),
            ("X", "2020-01-01", "2020-02-01", "present", "carried_value"),
        ],
        ["asset_id", "start", "end", "ownership", "upstream_col"],
    )
    result = TemporalToolkit.dedupe_segment_rows(
        df,
        ["asset_id", "start", "end"],
        unit_cols=["ownership"],
        pass_through_cols=["upstream_col"],
    )
    row = result.collect()[0]
    assert row["ownership"] == "present"
    assert row["upstream_col"] == "carried_value"


def test_dedupe_segment_rows_is_deterministic_across_row_order():
    rows = [
        ("X", "2020-01-01", "2020-02-01", None, "pairs_A"),
        ("X", "2020-01-01", "2020-02-01", 0.7, None),
    ]
    df1 = spark.createDataFrame(
        rows, ["asset_id", "start", "end", "ownership", "pairs_with_ownership"]
    )
    df2 = spark.createDataFrame(
        list(reversed(rows)),
        ["asset_id", "start", "end", "ownership", "pairs_with_ownership"],
    )

    result1 = TemporalToolkit.dedupe_segment_rows(
        df1,
        ["asset_id", "start", "end"],
        unit_cols=["ownership", "pairs_with_ownership"],
    ).collect()[0]
    result2 = TemporalToolkit.dedupe_segment_rows(
        df2,
        ["asset_id", "start", "end"],
        unit_cols=["ownership", "pairs_with_ownership"],
    ).collect()[0]
    assert result1["ownership"] == result2["ownership"]
    assert result1["pairs_with_ownership"] == result2["pairs_with_ownership"]


def test_dedupe_segment_rows_partitions_independently_per_key():
    df = spark.createDataFrame(
        [
            ("X", "2020-01-01", "2020-02-01", 0.5),
            ("Y", "2020-01-01", "2020-02-01", 0.9),
        ],
        ["asset_id", "start", "end", "ownership"],
    )
    result = TemporalToolkit.dedupe_segment_rows(
        df, ["asset_id", "start", "end"], unit_cols=["ownership"]
    )
    assert result.count() == 2


# -------------------------------------
# collapse_segments
# -------------------------------------
def test_collapse_segments_merges_adjacent_same_state():
    df = spark.createDataFrame(
        [
            ("X", "2020-01-01", "2020-02-01", "A"),
            ("X", "2020-02-01", "2020-03-01", "A"),
        ],
        ["asset_id", "start", "end", "state"],
    )
    df = df.withColumn("start", F.to_timestamp("start")).withColumn(
        "end", F.to_timestamp("end")
    )
    result = TemporalToolkit.collapse_segments(
        df, ["asset_id"], ["state"], "start", "end"
    )
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["start"].strftime("%Y-%m-%d") == "2020-01-01"
    assert rows[0]["end"].strftime("%Y-%m-%d") == "2020-03-01"


def test_collapse_segments_does_not_merge_across_gap():
    df = spark.createDataFrame(
        [
            ("X", "2020-01-01", "2020-02-01", "A"),
            ("X", "2020-04-01", "2020-05-01", "A"),  # gap between 02-01 and 04-01
        ],
        ["asset_id", "start", "end", "state"],
    )
    df = df.withColumn("start", F.to_timestamp("start")).withColumn(
        "end", F.to_timestamp("end")
    )
    result = TemporalToolkit.collapse_segments(
        df, ["asset_id"], ["state"], "start", "end"
    )
    assert result.count() == 2


def test_collapse_segments_does_not_merge_across_state_change():
    df = spark.createDataFrame(
        [
            ("X", "2020-01-01", "2020-02-01", "A"),
            ("X", "2020-02-01", "2020-03-01", "B"),
        ],
        ["asset_id", "start", "end", "state"],
    )
    df = df.withColumn("start", F.to_timestamp("start")).withColumn(
        "end", F.to_timestamp("end")
    )
    result = TemporalToolkit.collapse_segments(
        df, ["asset_id"], ["state"], "start", "end"
    )
    assert result.count() == 2


def test_collapse_segments_merges_opened_by_sources_across_merged_rows():
    df = spark.createDataFrame(
        [
            ("X", "2020-01-01", "2020-02-01", "A", ["src1"]),
            ("X", "2020-02-01", "2020-03-01", "A", ["src2"]),
        ],
        ["asset_id", "start", "end", "state", "opened_by_sources"],
    )
    df = df.withColumn("start", F.to_timestamp("start")).withColumn(
        "end", F.to_timestamp("end")
    )
    result = TemporalToolkit.collapse_segments(
        df, ["asset_id"], ["state"], "start", "end"
    )
    row = result.collect()[0]
    assert sorted(row["opened_by_sources"]) == ["src1", "src2"]


def test_collapse_segments_partitions_independently_per_key():
    df = spark.createDataFrame(
        [
            ("X", "2020-01-01", "2020-02-01", "A"),
            ("Y", "2020-01-01", "2020-02-01", "A"),
        ],
        ["asset_id", "start", "end", "state"],
    )
    df = df.withColumn("start", F.to_timestamp("start")).withColumn(
        "end", F.to_timestamp("end")
    )
    result = TemporalToolkit.collapse_segments(
        df, ["asset_id"], ["state"], "start", "end"
    )
    assert result.count() == 2
