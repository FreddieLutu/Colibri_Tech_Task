from __future__ import annotations

import argparse
import logging
import sys
from typing import Sequence

from .logging_setup import configure_logging

logger = logging.getLogger(__name__)


# --- shared helpers ------------------------------------------------------
def _build_databricks_spark():
    """
    Return the SparkSession to use inside a job task.

    On a Databricks cluster the entry point is already attached to a running
    SparkContext; ``SparkSession.builder.getOrCreate()`` simply hands it back.
    Keeping this in one place means a local override (e.g. for integration
    tests) only has to be made once.
    """
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def _argparser(prog: str) -> argparse.ArgumentParser:

    p = argparse.ArgumentParser(prog=prog)
    p.add_argument(
        "--catalog",
        required=True,
        help="Unity Catalog to read source files from and write Delta tables to.",
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return p


# --- per-stage entry points ---------------------------------------------
def ingest_clean(argv: Sequence[str] | None = None) -> int:
    """
    Bronze read + silver cleanse. Writes ``<catalog>.silver.power_readings``.
    """
    parser = _argparser("ingest_clean")
    parser.add_argument(
        "--source-volume",
        required=True,
        help="UC Volume path containing data_group_*.csv files.",
    )
    args = parser.parse_args(argv)
    configure_logging(args.log_level)
    logger.info("ingest_clean | catalog=%s | source=%s", args.catalog, args.source_volume)

    spark = _build_databricks_spark()
    from .bronze.ingest import read_raw
    from .silver.clean import clean

    bronze = read_raw(spark, args.source_volume)
    silver = clean(bronze)
    target = f"{args.catalog}.silver.power_readings"
    (
        silver.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target)
    )
    logger.info("ingest_clean wrote table=%s rows=%d", target, silver.count())
    return 0


def summarise(argv: Sequence[str] | None = None) -> int:
    """
    Gold daily summary. Writes ``<catalog>.gold.daily_summary``.
    """

    args = _argparser("summarise").parse_args(argv)
    configure_logging(args.log_level)
    logger.info("summarise | catalog=%s", args.catalog)

    spark = _build_databricks_spark()
    from .gold.stats import summarise as _summarise

    silver = spark.table(f"{args.catalog}.silver.power_readings")
    summary = _summarise(silver)
    target = f"{args.catalog}.gold.daily_summary"
    summary.write.format("delta").mode("overwrite").saveAsTable(target)
    logger.info("summarise wrote table=%s rows=%d", target, summary.count())
    return 0


def detect_anomalies(argv: Sequence[str] | None = None) -> int:
    """
    Gold anomaly detection. Writes ``<catalog>.gold.daily_anomalies``.
    """
    
    parser = _argparser("detect_anomalies")
    parser.add_argument(
        "--std-threshold",
        type=float,
        default=2.0,
        help="Number of stddevs from fleet mean to flag as anomaly.",
    )
    args = parser.parse_args(argv)
    configure_logging(args.log_level)
    logger.info(
        "detect_anomalies | catalog=%s | threshold=%.1f", args.catalog, args.std_threshold
    )

    spark = _build_databricks_spark()
    from .gold.anomalies import detect_anomalies as _detect

    summary = spark.table(f"{args.catalog}.gold.daily_summary")
    anomalies = _detect(summary, args.std_threshold)
    target = f"{args.catalog}.gold.daily_anomalies"
    anomalies.write.format("delta").mode("overwrite").saveAsTable(target)
    logger.info(
        "detect_anomalies wrote table=%s rows=%d flagged=%d",
        target,
        anomalies.count(),
        anomalies.where("is_anomaly = true").count(),
    )
    return 0


# Allow `python -m wind_pipeline.entrypoints <stage> ...` for local debugging.
if __name__ == "__main__":
    stage = sys.argv[1] if len(sys.argv) > 1 else ""
    handlers = {
        "ingest_clean": ingest_clean,
        "summarise": summarise,
        "detect_anomalies": detect_anomalies,
    }
    if stage not in handlers:
        sys.exit(f"unknown stage {stage!r}; choose one of {list(handlers)}")
    raise SystemExit(handlers[stage](sys.argv[2:]))