import re
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from drive_failure_lakehouse.smart import (
    discover_smart_attributes,
    with_smart_attributes,
)

FIXTURE_DIRECTORY = Path(__file__).parents[1] / "fixtures"
WIDE_SMART_COLUMN_PATTERN = re.compile(r"^smart_\d+_(raw|normalized)$")


def read_fixture(spark: SparkSession, filename: str) -> DataFrame:
    """Read a representative Backblaze CSV with every value kept as text."""
    return spark.read.option("header", True).csv(str(FIXTURE_DIRECTORY / filename))


def test_discovers_numerically_sorted_attribute_numbers() -> None:
    columns = [
        "smart_9_raw",
        "model",
        "smart_10_normalized",
        "smart_1_normalized",
        "smart_1_raw",
        "smart_status",
    ]

    assert discover_smart_attributes(columns) == ("1", "9", "10")


def test_normalizes_different_wide_schemas_to_same_contract(
    spark: SparkSession,
) -> None:
    old = with_smart_attributes(read_fixture(spark, "schema_2019.csv"))
    new = with_smart_attributes(read_fixture(spark, "schema_2024.csv"))

    assert old.schema["smart_attributes"].dataType == new.schema["smart_attributes"].dataType
    assert not any(WIDE_SMART_COLUMN_PATTERN.fullmatch(name) for name in old.columns)
    assert not any(WIDE_SMART_COLUMN_PATTERN.fullmatch(name) for name in new.columns)


def test_preserves_numeric_smart_readings_in_stable_map(spark: SparkSession) -> None:
    row = with_smart_attributes(read_fixture(spark, "schema_2019.csv")).first()

    assert row is not None
    assert row.smart_attributes["9"].raw == 7123
    assert row.smart_attributes["9"].normalized == 92


def test_invalid_optional_smart_value_becomes_warning(spark: SparkSession) -> None:
    row = with_smart_attributes(read_fixture(spark, "schema_2024.csv")).first()

    assert row is not None
    assert row.smart_attributes["197"].raw is None
    assert row.smart_attributes["197"].normalized == 100
    assert "INVALID_SMART_197_RAW" in row.quality_warnings


def test_omits_attribute_when_both_readings_are_blank(spark: SparkSession) -> None:
    row = with_smart_attributes(read_fixture(spark, "schema_2024.csv")).first()

    assert row is not None
    assert "198" not in row.smart_attributes
