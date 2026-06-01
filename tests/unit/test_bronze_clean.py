from __future__ import annotations

from datetime import datetime
from typing import Callable

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

pytest.importorskip("wind_pipeline.clean", reason="implement clean module")
from wind_pipeline.clean import clean  # noqa: E402


def test_drops_rows_missing_key_fields(
    spark: SparkSession,
    raw_schema: StructType,
    ts: Callable[..., datetime],
) -> None:
    rows = [
        (ts(0), 1, 10.0, 180.0, 2.5),
        (None,  1, 10.0, 180.0, 2.5),     # missing timestamp
        (ts(1), None, 10.0, 180.0, 2.5),  # missing turbine_id
    ]
    df = spark.createDataFrame(rows, schema=raw_schema)
    out = clean(df).collect()
    assert len(out) == 1
    assert out[0]["turbine_id"] == 1


def test_imputes_impossible_and_missing_measurements(
    spark: SparkSession,
    raw_schema: StructType,
    ts: Callable[..., datetime],
) -> None:
    # Three valid values (median 2.0); two should be imputed back to 2.0.
    rows = [
        (ts(0), 1, 10.0, 180.0, 1.0),
        (ts(1), 1, 10.0, 180.0, 2.0),
        (ts(2), 1, 10.0, 180.0, 3.0),
        (ts(3), 1, 10.0, 180.0, -5.0),  # impossible → null → imputed
        (ts(4), 1, 10.0, 180.0, None),  # missing  → imputed
    ]
    df = spark.createDataFrame(rows, schema=raw_schema)
    out = {r["timestamp"]: r for r in clean(df).collect()}
    assert out[ts(3)]["power_output"] == 2.0
    assert out[ts(4)]["power_output"] == 2.0


def test_deduplicates_on_timestamp_and_turbine(
    spark: SparkSession,
    raw_schema: StructType,
    ts: Callable[..., datetime],
) -> None:
    # Daily appends can include overlapping rows; we must not double-count.
    rows = [
        (ts(0), 1, 10.0, 180.0, 2.5),
        (ts(0), 1, 10.0, 180.0, 2.5),
    ]
    df = spark.createDataFrame(rows, schema=raw_schema)
    assert clean(df).count() == 1