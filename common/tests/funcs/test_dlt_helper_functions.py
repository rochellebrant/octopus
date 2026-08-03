import os
import pytest
import sys
from unittest.mock import patch, MagicMock
from functools import wraps
from datetime import datetime, date

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StringType,
    StructField,
    IntegerType,
    DoubleType,
    TimestampType,
)
from collections import OrderedDict

# Import helper
import funcs.dlt_helper as dlt_helper
from funcs.dlt_helper import *
from funcs.dlt_functions import generate_scd_tables

spark = SparkSession.builder.getOrCreate()

# UTILITIES


def suppress_print(func):
    """Decorator to suppress print statements during function execution."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        with patch("builtins.print"):
            return func(*args, **kwargs)

    return wrapper


# TEST DEFINITIONS


@suppress_print
def test_cast_columns_date_success():
    df = spark.createDataFrame([("01-07-2023",)], "event_date string")
    col_defs = {
        "my_table": {"columns": {"event_date": {"schema": {"data_type": "DATE"}}}}
    }
    tbl_cfg = {"my_table": {"date_formats": {"event_date": "dd-MM-yyyy"}}}
    result_df = cast_columns(df, col_defs, tbl_cfg, "my_table")
    row = result_df.collect()[0]
    assert row["event_date"] == date(2023, 7, 1)


@suppress_print
def test_cast_columns_timestamp_success():
    df = spark.createDataFrame([("01-07-2023 14:45:00",)], "event_time string")
    col_defs = {
        "my_table": {"columns": {"event_time": {"schema": {"data_type": "TIMESTAMP"}}}}
    }
    tbl_cfg = {"my_table": {"timestamp_formats": {"event_time": "dd-MM-yyyy HH:mm:ss"}}}
    result_df = cast_columns(df, col_defs, tbl_cfg, "my_table")
    row = result_df.collect()[0]
    assert row["event_time"].strftime("%Y-%m-%d %H:%M:%S") == "2023-07-01 14:45:00"


@suppress_print
def test_cast_columns_to_numeric():
    df = spark.createDataFrame([("123", "45.67")], "int_col string, double_col string")
    col_defs = {
        "my_table": {
            "columns": {
                "int_col": {"schema": {"data_type": "INT"}},
                "double_col": {"schema": {"data_type": "DOUBLE"}},
            }
        }
    }
    result_df = cast_columns(df, col_defs, {"my_table": {}}, "my_table")
    row = result_df.collect()[0]
    assert row["int_col"] == 123 and abs(row["double_col"] - 45.67) < 0.001


@suppress_print
def test_cast_columns_date_format_type_error():
    df = spark.createDataFrame([("01-07-2023",)], "event_date string")
    col_defs = {
        "my_table": {"columns": {"event_date": {"schema": {"data_type": "DATE"}}}}
    }
    tbl_cfg = {"my_table": {"date_formats": "not_a_dict"}}
    with pytest.raises(AttributeError):
        cast_columns(df, col_defs, tbl_cfg, "my_table")


@suppress_print
def test_cast_columns_preserves_user_casing():
    """Verifies that user-defined casing is applied to the output columns."""
    df = spark.createDataFrame([("01-07-2023",)], "event_date string")
    col_defs = {
        "my_table": {"columns": {"EVENT_DATE": {"schema": {"data_type": "DATE"}}}}
    }
    tbl_cfg = {"my_table": {"date_formats": {"EVENT_DATE": "dd-MM-yyyy"}}}
    result_df = cast_columns(df, col_defs, tbl_cfg, "my_table")
    assert "EVENT_DATE" in result_df.columns
    assert result_df.collect()[0]["EVENT_DATE"] == date(2023, 7, 1)


@suppress_print
def test_cast_columns_conflicting_timestamp_type():
    df = spark.createDataFrame([("01-07-2023",)], "event_date string")
    col_defs = {
        "my_table": {"columns": {"event_date": {"schema": {"data_type": "STRING"}}}}
    }
    tbl_cfg = {"my_table": {"timestamp_formats": {"event_date": "dd-MM-yyyy"}}}
    with pytest.raises(ValueError) as excinfo:
        cast_columns(df, col_defs, tbl_cfg, "my_table")
    assert "timestamp format" in str(excinfo.value).lower()


@suppress_print
def test_cast_columns_timestamp_format_type_error():
    df = spark.createDataFrame([("01-07-2023",)], "event_date string")
    col_defs = {
        "my_table": {"columns": {"event_date": {"schema": {"data_type": "TIMESTAMP"}}}}
    }
    tbl_cfg = {"my_table": {"timestamp_formats": "not_a_dict"}}
    with pytest.raises(AttributeError):
        cast_columns(df, col_defs, tbl_cfg, "my_table")


@suppress_print
def test_cast_columns_with_invalid_numeric_cast():
    df = spark.createDataFrame([("abc",)], "int_col string")
    col_defs = {"my_table": {"columns": {"int_col": {"schema": {"data_type": "INT"}}}}}
    casted_df = cast_columns(df, col_defs, {"my_table": {}}, "my_table")
    with pytest.raises(Exception) as excinfo:
        casted_df.collect()
    assert "CAST_INVALID_INPUT" in str(excinfo.value)


@suppress_print
def test_filter_quarantine_clean():
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("DQ_hard_null_col1", IntegerType(), True),
            StructField("DQ_soft_join_check", IntegerType(), True),
            StructField("other_data", StringType(), True),
        ]
    )
    test_data = [(1, 0, 0, "clean"), (2, 1, 0, "hard_fail"), (3, 0, 1, "soft_fail")]
    df = spark.createDataFrame(test_data, schema)

    quarantine_df, clean_df = filter_quarantine_clean(df)

    q_ids = [r["id"] for r in quarantine_df.collect()]
    c_ids = [r["id"] for r in clean_df.collect()]

    # 1 and 3 should be in clean (Row 3 is just a soft fail)
    # 2 and 3 should be in quarantine (Row 3 is flagged for review)
    assert sorted(q_ids) == [2, 3]
    assert sorted(c_ids) == [1, 3]
    assert not any(c.startswith("DQ_") for c in clean_df.columns)


@suppress_print
def test_apply_hard_check_flags_legacy_string_rule_defaults_hard():
    """Plain string rules (legacy shorthand) must still produce a DQ_hard_ flag."""
    df = spark.createDataFrame([(1,), (-1,)], "value int")
    table_rules = {"negative_value": "value < 0"}

    result_df = apply_hard_check_flags(df, table_rules)

    assert "DQ_hard_negative_value" in result_df.columns
    assert "DQ_soft_negative_value" not in result_df.columns
    flags = {r["value"]: r["DQ_hard_negative_value"] for r in result_df.collect()}
    assert flags == {1: 0, -1: 1}


@suppress_print
def test_apply_hard_check_flags_soft_opt_in():
    """A rule dict with hard_check=False should produce a DQ_soft_ flag instead."""
    df = spark.createDataFrame([(1,), (-1,)], "value int")
    table_rules = {"negative_value": {"expr": "value < 0", "hard_check": False}}

    result_df = apply_hard_check_flags(df, table_rules)

    assert "DQ_soft_negative_value" in result_df.columns
    assert "DQ_hard_negative_value" not in result_df.columns
    flags = {r["value"]: r["DQ_soft_negative_value"] for r in result_df.collect()}
    assert flags == {1: 0, -1: 1}


@suppress_print
def test_apply_hard_check_flags_mixed_hard_and_soft():
    """Hard and soft rules on the same table should each get their own flag column."""
    df = spark.createDataFrame([(1,), (-1,)], "value int")
    table_rules = {
        "negative_value": "value < 0",
        "not_one": {"expr": "value != 1", "hard_check": False},
    }

    result_df = apply_hard_check_flags(df, table_rules)

    assert "DQ_hard_negative_value" in result_df.columns
    assert "DQ_soft_not_one" in result_df.columns


@suppress_print
def test_cast_columns_variant_quotes_and_native_iso():
    """Verifies that literal JSON quotes are stripped and native Spark ISO casting handles the 'T'."""
    # Simulating a string extracted directly from a Variant payload
    df = spark.createDataFrame([('"2020-01-01T14:45:00"',)], "event_time string")
    col_defs = {
        "my_table": {"columns": {"event_time": {"schema": {"data_type": "TIMESTAMP"}}}}
    }

    # We pass an empty dict for table_config to force the native fallback
    result_df = cast_columns(df, col_defs, {"my_table": {}}, "my_table")
    row = result_df.collect()[0]

    assert row["event_time"].strftime("%Y-%m-%d %H:%M:%S") == "2020-01-01 14:45:00"


@suppress_print
def test_build_static_schema_success():
    """Verifies the static schema builder constructs valid DDL strings from dictionaries."""
    col_defs = {
        "my_table": {
            "columns": {
                "company_id": {
                    "schema": {"data_type": "STRING"},
                    "description": {"comment": "The ID"},
                },
                "date_of_transaction": {
                    "schema": {"data_type": "TIMESTAMP"},
                    "description": {},  # Missing comment should just skip the COMMENT block
                },
            }
        }
    }

    result = build_static_schema("my_table", col_defs)
    expected = "`company_id` STRING COMMENT 'The ID', `date_of_transaction` TIMESTAMP"
    assert result == expected


@suppress_print
def test_build_static_schema_empty_or_none():
    """Verifies the static schema builder handles missing or empty configs gracefully."""
    assert build_static_schema("my_table", None) is None
    assert build_static_schema("my_table", {}) is None
    assert build_static_schema("my_table", {"other_table": {}}) is None


@suppress_print
@patch("funcs.dlt_functions.dlt")
def test_generate_scd_tables_without_deletes(mock_dlt):
    """
    Verifies that the default behavior of generate_scd_tables works correctly
    and does NOT inject the apply_as_deletes argument if it isn't configured.
    """
    table_config = {
        "my_table": {
            "load_date_column": "update_timestamp",
            "match_keys": [
                "company_id",
                "update_timestamp",
            ],  # update_timestamp should be filtered out
            "audit_keys": ["refresh_timestamp"],
        }
    }

    # Execute the function
    generate_scd_tables(
        spark=spark,
        source_table_prefix="bronze_",
        table="my_table",
        table_config=table_config,
        column_defintions=None,  # Bypassing schema generation for this test
        input="landing_",
        output="bronze_",
        defined_schema=False,
    )

    # Verify the DLT creation was called
    mock_dlt.create_streaming_table.assert_called_once()

    # Verify apply_changes was called correctly without the delete argument
    mock_dlt.apply_changes.assert_called_once()
    called_kwargs = mock_dlt.apply_changes.call_args[1]

    assert called_kwargs["target"] == "bronze_my_table"
    assert called_kwargs["source"] == "v_scd_src_my_table"
    assert called_kwargs["keys"] == [
        "company_id"
    ]  # Ensured the sequence column was stripped from keys
    assert called_kwargs["sequence_by"] == "update_timestamp"
    assert (
        "apply_as_deletes" not in called_kwargs
    )  # Crucial backward-compatibility check


@suppress_print
@patch("funcs.dlt_functions.dlt")
def test_generate_scd_tables_with_deletes(mock_dlt):
    """
    Verifies the opt-in tombstoning functionality successfully maps the
    apply_as_deletes parameter into the DLT apply_changes kwargs.
    """
    table_config = {
        "my_table": {
            "load_date_column": "source_ingestion_timestamp",
            "match_keys": ["company_id"],
            "apply_as_deletes_expr": "is_deleted = True",  # Opt-in configuration
        }
    }

    # Execute the function
    generate_scd_tables(
        spark=spark,
        source_table_prefix="bronze_",
        table="my_table",
        table_config=table_config,
        column_defintions=None,
        input="landing_",
        output="bronze_",
        defined_schema=False,
    )

    # Verify apply_changes was called correctly WITH the delete argument
    mock_dlt.apply_changes.assert_called_once()
    called_kwargs = mock_dlt.apply_changes.call_args[1]

    assert "apply_as_deletes" in called_kwargs
    assert called_kwargs["apply_as_deletes"] == "is_deleted = true"
