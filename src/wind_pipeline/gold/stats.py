"""Gold layer: per-turbine summary statistics over the configured period.

  The brief asks for min / max / avg power output per turbine over a 24-hour
  window. We bucket by calendar day (UTC) — it matches how operations teams
  typically report and keeps the join key trivial for the anomaly stage that
  consumes this table.
  """
  
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def summarise(cleaned: DataFrame) -> DataFrame:
    return (
        cleaned.withColumn("period_date", F.to_date("timestamp"))
        .groupBy("turbine_id", "period_date")
        .agg(
            F.min("power_output").alias("min_power_output"),
            F.max("power_output").alias("max_power_output"),
            F.avg("power_output").alias("avg_power_output"),
            F.count(F.lit(1)).alias("sample_count"),
        )
    )