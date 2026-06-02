"""Shared fixtures for the wind_pipeline test pack.

Provides a Spark session that switches between:

* a local PySpark session (fast, no workspace)
* a Databricks Connect session (`DB_CONNECT=1` env var), so the same tests
  run against a real DBR cluster.

The schema and timestamp helpers below are deliberately tiny — keep tests
readable, push setup into fixtures.
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Callable, Iterator

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    # Inside Databricks (Serverless or DBR with Connect) a session is already
    # available via SPARK_REMOTE — we must NOT try to set master() there or
    # Spark refuses with CANNOT_CONFIGURE_SPARK_CONNECT_MASTER.
    on_databricks = "SPARK_REMOTE" in os.environ or os.environ.get("DB_CONNECT") == "1"

    if on_databricks:
        session = SparkSession.builder.getOrCreate()
        yield session
        return  # don't stop a shared remote session

    # Local dev only: spin up an in-process Spark.
    session = (
        SparkSession.builder.appName("wind-pipeline-tests")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def raw_schema() -> StructType:
    return StructType(
        [
            StructField("timestamp", TimestampType(), True),
            StructField("turbine_id", IntegerType(), True),
            StructField("wind_speed", DoubleType(), True),
            StructField("wind_direction", DoubleType(), True),
            StructField("power_output", DoubleType(), True),
        ]
    )


@pytest.fixture
def ts() -> Callable[..., datetime]:
    """Tiny timestamp builder so test bodies stay readable."""

    def _ts(hour: int = 0, day: int = 1) -> datetime:
        return datetime(2022, 3, day, hour, 0, 0)

    return _ts

# import os
# import sys
# from datetime import datetime
# from pathlib import Path
# from typing import Callable, Iterator

# import pytest
# from pyspark.sql import SparkSession
# from pyspark.sql.types import (
#     DoubleType,
#     IntegerType,
#     StructField,
#     StructType,
#     TimestampType,
# )


# # --- make wind_pipeline importable from src/ -----------------------------
# # Walk up from this conftest until we find a sibling `src/wind_pipeline/`
# # directory and put `src/` on sys.path. This avoids hard-coding a workspace
# # path like /Workspace/Users/<you>/... and works locally too.
# _here = Path(__file__).resolve()
# for _parent in _here.parents:
#     _candidate = _parent / "src"
#     if (_candidate / "wind_pipeline").is_dir():
#         if str(_candidate) not in sys.path:
#             sys.path.insert(0, str(_candidate))
#         break
# # -------------------------------------------------------------------------


# @pytest.fixture(scope="session")
#   def spark() -> Iterator[SparkSession]:
#       # Inside Databricks (Serverless or DBR with Connect) a session is already
#       # available via SPARK_REMOTE — we must NOT try to set master() there or
#       # Spark refuses with CANNOT_CONFIGURE_SPARK_CONNECT_MASTER.
#       on_databricks = "SPARK_REMOTE" in os.environ or os.environ.get("DB_CONNECT") == "1"

#       if on_databricks:
#           session = SparkSession.builder.getOrCreate()
#           yield session
#           return  # don't stop a shared remote session

#       # Local dev: spin up an in-process Spark.
#       session = (
#           SparkSession.builder.appName("wind-pipeline-tests")
#           .master("local[2]")
#           .config("spark.sql.session.timeZone", "UTC")
#           .config("spark.sql.shuffle.partitions", "2")
#           .config("spark.ui.enabled", "false")
#           .getOrCreate()
#       )
#       session.sparkContext.setLogLevel("ERROR")
#       yield session
#       session.stop()


# @pytest.fixture
# def raw_schema() -> StructType:
#     return StructType(
#         [
#             StructField("timestamp", TimestampType(), True),
#             StructField("turbine_id", IntegerType(), True),
#             StructField("wind_speed", DoubleType(), True),
#             StructField("wind_direction", DoubleType(), True),
#             StructField("power_output", DoubleType(), True),
#         ]
#     )


# @pytest.fixture
# def ts() -> Callable[..., datetime]:
#     """Tiny timestamp builder so test bodies stay readable."""

#     def _ts(hour: int = 0, day: int = 1) -> datetime:
#         return datetime(2022, 3, day, hour, 0, 0)

#     return _ts
