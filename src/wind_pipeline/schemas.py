"""
Raw schema for incoming turbine telemetry CSVs.

Locked here so every downstream stage agrees on column names and types. If
the CSVs ever change shape the test_schemas tests fail loudly, which is
exactly the safety net we want.
"""

# def detect_schema(summary):
#     return summary

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)

RAW_SCHEMA: StructType = StructType(
    [
        StructField("timestamp", TimestampType(), nullable=True),
        StructField("turbine_id", IntegerType(), nullable=True),
        StructField("wind_speed", DoubleType(), nullable=True),
        StructField("wind_direction", DoubleType(), nullable=True),
        StructField("power_output", DoubleType(), nullable=True),
    ]
)