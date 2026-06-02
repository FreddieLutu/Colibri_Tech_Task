"""
Gold layer: anomaly detection.

The brief defines an anomaly as a turbine whose output is outside 2 standard
deviations from the mean over the period. We interpret "the mean" as the
fleet-wide mean for the same day, but with the **candidate turbine excluded**
from the reference distribution (leave-one-out).

Why leave-one-out: with a small fleet (15 turbines, often fewer reporting on a
given day), a single severe outlier drags the naive cross-sectional mean
toward itself and inflates the naive σ — enough to keep its own z-score below
2 and mask itself. Excluding the candidate removes that self-masking.

Implementation uses sum and sum-of-squares over a day window, then subtracts
the current row's contribution to get leave-one-out moments in O(N) without
a self-join.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

ANOMALY_STD_THRESHOLD: float = 2.0


def detect_anomalies(
    summary: DataFrame, std_threshold: float = ANOMALY_STD_THRESHOLD
) -> DataFrame:
    day = Window.partitionBy("period_date")

    x = F.col("avg_power_output")
    n_loo = F.count(F.lit(1)).over(day) - F.lit(1)
    sum_loo = F.sum("avg_power_output").over(day) - x
    sumsq_loo = F.sum(x * x).over(day) - x * x

    mean_loo = F.when(n_loo > 0, sum_loo / n_loo)
    # Sample variance with Bessel's correction needs N >= 2 in the LOO set,
    # i.e. at least 3 turbines reporting on that day.
    var_loo = F.when(
        n_loo > 1,
        (sumsq_loo - n_loo * mean_loo * mean_loo) / (n_loo - F.lit(1)),
    )
    # Clamp tiny negatives that arise from float cancellation when all
    # leave-one-out values are equal.
    std_loo = F.when(var_loo.isNotNull(), F.sqrt(F.greatest(var_loo, F.lit(0.0))))

    annotated = (
        summary.withColumn("fleet_mean_power", mean_loo)
        .withColumn("fleet_std_power", std_loo)
        .withColumn(
            "z_score",
            F.when(
                (F.col("fleet_std_power").isNotNull())
                & (F.col("fleet_std_power") > 0),
                (F.col("avg_power_output") - F.col("fleet_mean_power"))
                / F.col("fleet_std_power"),
            ),
        )
        .withColumn(
            "is_anomaly",
            F.when(F.abs(F.col("z_score")) > F.lit(std_threshold), F.lit(True))
            .otherwise(F.lit(False)),
        )
    )

    return annotated.select(
        "turbine_id",
        "period_date",
        "avg_power_output",
        "fleet_mean_power",
        "fleet_std_power",
        "z_score",
        "is_anomaly",
    )