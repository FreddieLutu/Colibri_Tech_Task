# Databricks notebook source
# MAGIC %md
# MAGIC # TDD step 4 — gold anomaly detection
# MAGIC
# MAGIC Three behaviours to lock in. The 5-turbine clustered case is the
# MAGIC canonical sanity check; the per-day isolation and zero-variance cases
# MAGIC are the gotchas that small fleets will trip on.
# MAGIC
# MAGIC If your first test passes but the others don't, you probably
# MAGIC implemented the naive cross-sectional z-score. Switch to
# MAGIC **leave-one-out** moments so the candidate doesn't contaminate its
# MAGIC own reference distribution.

# COMMAND ----------
# MAGIC %pip install -e /Workspace/Users/freddielutu@gmail.com/Colibri_Tech_Task
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
from datetime import date

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StructField,
    StructType,
)

pytest.importorskip("wind_pipeline.gold.anomalies", reason="implement anomalies module")
from wind_pipeline.gold.anomalies import detect_anomalies  # noqa: E402

SummaryRow = tuple[int, date, float, float, float, int]

SUMMARY_SCHEMA: StructType = StructType(
    [
        StructField("turbine_id", IntegerType()),
        StructField("period_date", DateType()),
        StructField("min_power_output", DoubleType()),
        StructField("max_power_output", DoubleType()),
        StructField("avg_power_output", DoubleType()),
        StructField("sample_count", LongType()),
    ]
)


def _row(turbine_id: int, avg: float, day: int = 1) -> SummaryRow:
    return (turbine_id, date(2022, 3, day), avg, avg, avg, 24)


# COMMAND ----------
def test_flags_turbine_outside_two_sigma(spark: SparkSession) -> None:
    rows: list[SummaryRow] = [_row(1, 2.4), _row(2, 2.5), _row(3, 2.6), _row(4, 2.5), _row(5, 10.0)]
    df = spark.createDataFrame(rows, schema=SUMMARY_SCHEMA)
    out = {r["turbine_id"]: r for r in detect_anomalies(df).collect()}
    assert out[5]["is_anomaly"] is True
    for tid in (1, 2, 3, 4):
        assert out[tid]["is_anomaly"] is False


# COMMAND ----------
def test_per_day_isolation(spark: SparkSession) -> None:
    # Day 1 has an outlier; day 2 is all-equal — must not flag day 2.
    rows: list[SummaryRow] = [
        _row(1, 1.9, day=1), _row(2, 2.0, day=1), _row(3, 10.0, day=1),
        _row(1, 2.0, day=2), _row(2, 2.0, day=2), _row(3, 2.0, day=2),
    ]
    df = spark.createDataFrame(rows, schema=SUMMARY_SCHEMA)
    out = {(r["turbine_id"], r["period_date"]): r for r in detect_anomalies(df).collect()}
    assert out[(3, date(2022, 3, 1))]["is_anomaly"] is True
    assert all(out[(t, date(2022, 3, 2))]["is_anomaly"] is False for t in (1, 2, 3))


# COMMAND ----------
def test_zero_variance_day_yields_no_anomaly(spark: SparkSession) -> None:
    # Edge case: if every turbine is identical, σ=0 and z-score is undefined.
    # We must not flag anyone — silently inventing anomalies is the worst bug.
    rows: list[SummaryRow] = [_row(1, 2.5), _row(2, 2.5), _row(3, 2.5)]
    df = spark.createDataFrame(rows, schema=SUMMARY_SCHEMA)
    out = detect_anomalies(df).collect()
    assert all(r["is_anomaly"] is False for r in out)