from __future__ import annotations

from dataclasses import dataclass, field
from typing import List
from pyspark.sql.types import StructType, StructField, StringType, ArrayType


class TableCommentsError(RuntimeError):
    """Raised when comment application fails in a way that should fail the run."""
    pass


@dataclass
class TableRunResult:
    table_key: str
    full_table_name: str
    status: str  # "ok" | "skipped" | "error" | "ok_with_warnings"
    message: str = ""
    undefined_columns: List[str] = field(default_factory=list)      # in table, not in defs
    defined_but_missing: List[str] = field(default_factory=list)    # in defs, not in table

    def add_message(self, msg: str) -> None:
        msg = (msg or "").strip()
        if not msg:
            return
        if self.message:
            self.message = f"{self.message} {msg}"
        else:
            self.message = msg

    def mark_warning(self) -> None:
        if self.status == "ok":
            self.status = "ok_with_warnings"


@dataclass
class RunSummary:
    results: List[TableRunResult] = field(default_factory=list)

    def add(self, table_key: str, full_table_name: str, status: str, message: str = "") -> TableRunResult:
        r = TableRunResult(table_key, full_table_name, status, message)
        self.results.append(r)
        return r

    @property
    def errors(self) -> List[TableRunResult]:
        return [r for r in self.results if r.status == "error"]

    @property
    def skipped(self) -> List[TableRunResult]:
        return [r for r in self.results if r.status == "skipped"]

    @property
    def ok(self) -> List[TableRunResult]:
        # includes ok_with_warnings as "ok" category for summary purposes
        return [r for r in self.results if r.status in ("ok", "ok_with_warnings")]

    def to_spark_df(self, spark):
        schema = StructType([
            StructField("table_key", StringType(), True),
            StructField("full_table_name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("message", StringType(), True),
            StructField("undefined_columns", ArrayType(StringType()), True),
            StructField("defined_but_missing", ArrayType(StringType()), True),
        ])

        rows = [
            (
                str(r.table_key) if r.table_key is not None else None,
                str(r.full_table_name) if r.full_table_name is not None else None,
                str(r.status) if r.status is not None else None,
                str(r.message) if r.message is not None else None,
                [str(x) for x in (r.undefined_columns or [])],
                [str(x) for x in (r.defined_but_missing or [])],
            )
            for r in self.results
        ]

        return spark.createDataFrame(rows, schema=schema)
