import os
import sys
from datetime import datetime
from pathlib import Path
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


# --- make wind_pipeline importable from src/ -----------------------------
# Walk up from this conftest until we find a sibling `src/wind_pipeline/`
# directory and put `src/` on sys.path. This avoids hard-coding a workspace
# path like /Workspace/Users/<you>/... and works locally too.
_here = Path(__file__).resolve()
for _parent in _here.parents:
    _candidate = _parent / "src"
    if (_candidate / "wind_pipeline").is_dir():
        if str(_candidate) not in sys.path:
            sys.path.insert(0, str(_candidate))
        break
# -------------------------------------------------------------------------


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    session: SparkSession
    if os.environ.get("DB_CONNECT") == "1":
        from databricks.connect import DatabricksSession

        session = DatabricksSession.builder.getOrCreate()
    else:
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

    if os.environ.get("DB_CONNECT") != "1":
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
