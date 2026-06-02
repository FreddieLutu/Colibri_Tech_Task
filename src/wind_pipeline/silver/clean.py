"""
Silver layer: clean raw turbine telemetry.

Cleaning rules:
1. Drop rows missing a key field (timestamp or turbine_id).
2. Null out impossible domain values so a single bad column doesn't lose
    the whole row.
3. Impute remaining nulls with the per-turbine median (robust to spikes).
4. Deduplicate on (timestamp, turbine_id) — daily appends can overlap.

We deliberately do NOT strip statistical outliers here. That would mask the
very deviations the anomaly stage is built to surface; cleaning only removes
*impossible* values, not unusual ones.
"""

# def detect_clean(summary):
#     return summary

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Physical / domain bounds for sensor readings. Values outside these ranges
# are treated as sensor errors and re-imputed from the turbine's own median.
POWER_OUTPUT_MIN_MW: float = 0.0
POWER_OUTPUT_MAX_MW: float = 5.0
WIND_SPEED_MIN_MS: float = 0.0
WIND_SPEED_MAX_MS: float = 50.0
WIND_DIRECTION_MIN_DEG: float = 0.0
WIND_DIRECTION_MAX_DEG: float = 360.0

_MEASUREMENT_COLS = ("wind_speed", "wind_direction", "power_output")


def _drop_missing_keys(df: DataFrame) -> DataFrame:
    return df.where(F.col("timestamp").isNotNull() & F.col("turbine_id").isNotNull())


def _null_impossible_values(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "power_output",
            F.when(
                (F.col("power_output") >= POWER_OUTPUT_MIN_MW)
                & (F.col("power_output") <= POWER_OUTPUT_MAX_MW),
                F.col("power_output"),
            ),
        )
        .withColumn(
            "wind_speed",
            F.when(
                (F.col("wind_speed") >= WIND_SPEED_MIN_MS)
                & (F.col("wind_speed") <= WIND_SPEED_MAX_MS),
                F.col("wind_speed"),
            ),
        )
        .withColumn(
            "wind_direction",
            F.when(
                (F.col("wind_direction") >= WIND_DIRECTION_MIN_DEG)
                & (F.col("wind_direction") <= WIND_DIRECTION_MAX_DEG),
                F.col("wind_direction"),
            ),
        )
    )


def _impute_with_turbine_median(df: DataFrame) -> DataFrame:
    """Fill nulls with the per-turbine median of each measurement column."""
    window = Window.partitionBy("turbine_id")
    out = df
    for col in _MEASUREMENT_COLS:
        median = F.expr(f"percentile_approx({col}, 0.5)")
        out = out.withColumn(col, F.coalesce(F.col(col), median.over(window)))
    return out


def clean(df: DataFrame) -> DataFrame:
    return (
        df.transform(_drop_missing_keys)
        .transform(_null_impossible_values)
        .transform(_impute_with_turbine_median)
        .dropDuplicates(["timestamp", "turbine_id"])
    )