# Databricks notebook source
# MAGIC %md
# MAGIC # TDD step 3 — gold summary statistics
# MAGIC
# MAGIC Smallest possible fixture that distinguishes min, max and avg per
# MAGIC turbine per calendar day.


# COMMAND ----------
# MAGIC %pip install -e /Workspace/Users/freddielutu@gmail.com/Colibri_Tech_Task
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
from datetime import date, datetime
from typing import Callable

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

pytest.importorskip("wind_pipeline.stats", reason="implement stats module")
from wind_pipeline.stats import summarise  # noqa: E402


# COMMAND ----------
def test_min_max_avg_per_turbine_per_day(
    spark: SparkSession,
    raw_schema: StructType,
    ts: Callable[..., datetime],
) -> None:
    rows = [
        (ts(hour=0,  day=1), 1, 10.0, 180.0, 1.0),
        (ts(hour=12, day=1), 1, 10.0, 180.0, 3.0),
        (ts(hour=0,  day=2), 1, 10.0, 180.0, 2.0),
        (ts(hour=0,  day=1), 2, 10.0, 180.0, 5.0),
    ]
    df = spark.createDataFrame(rows, schema=raw_schema)
    summary = {(r["turbine_id"], r["period_date"]): r for r in summarise(df).collect()}

    t1_d1 = summary[(1, date(2022, 3, 1))]
    assert (t1_d1["min_power_output"], t1_d1["max_power_output"], t1_d1["avg_power_output"]) == (1.0, 3.0, 2.0)
    assert t1_d1["sample_count"] == 2