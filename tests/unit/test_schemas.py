# Databricks notebook source
# MAGIC %md
# MAGIC # TDD step 1 — pin the raw schema
# MAGIC
# MAGIC Every downstream test assumes these column names and types. If the
# MAGIC schema drifts these tests fail loudly, before anything else does.

  
# COMMAND ----------
# MAGIC %pip install -e /Workspace/Users/freddielutu@gmail.com/Colibri_Tech_Task
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
import pytest
from pyspark.sql.types import StructField

pytest.importorskip("wind_pipeline.schemas", reason="implement schemas module")
from wind_pipeline.schemas import RAW_SCHEMA  # noqa: E402


# COMMAND ----------
def test_raw_schema_has_expected_fields() -> None:
    fields: dict[str, str] = {
        f.name: f.dataType.simpleString() for f in RAW_SCHEMA.fields
    }
    assert fields == {
        "timestamp": "timestamp",
        "turbine_id": "int",
        "wind_speed": "double",
        "wind_direction": "double",
        "power_output": "double",
    }


def test_raw_schema_allows_nulls_in_measurements() -> None:
    by_name: dict[str, StructField] = {f.name: f for f in RAW_SCHEMA.fields}
    # Measurements can be null — that's exactly what cleaning will impute.
    assert by_name["power_output"].nullable
    assert by_name["wind_speed"].nullable
    assert by_name["wind_direction"].nullable