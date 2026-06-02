"""
Bronze layer: ingest raw turbine CSVs from a Unity Catalog Volume.
Reads every ``data_group_*.csv`` under the supplied path using the explicit
raw schema. Adds an ``ingestion_file`` column for lineage. Reading by
directory glob means new daily appends are picked up automatically — drop a
new CSV into the Volume and the next run sees it.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from wind_pipeline.schemas import RAW_SCHEMA


def read_raw(spark: SparkSession, source_path: str) -> DataFrame:
    """Read all CSVs in ``source_path`` with the enforced schema."""
    pattern = f"{source_path.rstrip('/')}/*.csv"
    return (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(RAW_SCHEMA)
        .csv(pattern)
        .withColumn("ingestion_file", F.col("_metadata.file_path"))
    )