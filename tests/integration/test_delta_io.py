from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from drive_failure_lakehouse.delta_io import merge_delta_path


def read_rows(spark: SparkSession, path: Path) -> list[tuple[str, str, int]]:
    rows = spark.read.format("delta").load(str(path)).orderBy("date", "serial_number").collect()
    return [(row.date, row.serial_number, row.failure) for row in rows]


def test_replaying_source_does_not_duplicate_delta_rows(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    target = tmp_path / "replay-target"
    source = spark.createDataFrame(
        [
            ("2024-01-01", "SERIAL-A", 0),
            ("2024-01-01", "SERIAL-B", 1),
        ],
        ["date", "serial_number", "failure"],
    )

    merge_delta_path(spark, source, str(target), keys=("date", "serial_number"))
    merge_delta_path(spark, source, str(target), keys=("date", "serial_number"))

    assert read_rows(spark, target) == [
        ("2024-01-01", "SERIAL-A", 0),
        ("2024-01-01", "SERIAL-B", 1),
    ]


def test_correction_updates_existing_key_and_inserts_new_key(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    target = tmp_path / "correction-target"
    initial = spark.createDataFrame(
        [
            ("2024-01-01", "SERIAL-A", 0),
            ("2024-01-01", "SERIAL-B", 0),
        ],
        ["date", "serial_number", "failure"],
    )
    correction = spark.createDataFrame(
        [
            ("2024-01-01", "SERIAL-A", 1),
            ("2024-01-02", "SERIAL-C", 0),
        ],
        ["date", "serial_number", "failure"],
    )

    merge_delta_path(spark, initial, str(target), keys=("date", "serial_number"))
    merge_delta_path(spark, correction, str(target), keys=("date", "serial_number"))

    assert read_rows(spark, target) == [
        ("2024-01-01", "SERIAL-A", 1),
        ("2024-01-01", "SERIAL-B", 0),
        ("2024-01-02", "SERIAL-C", 0),
    ]


def test_rejects_duplicate_source_keys(spark: SparkSession, tmp_path: Path) -> None:
    source = spark.createDataFrame(
        [
            ("2024-01-01", "SERIAL-A", 0),
            ("2024-01-01", "SERIAL-A", 1),
        ],
        ["date", "serial_number", "failure"],
    )

    with pytest.raises(ValueError, match="duplicate merge keys"):
        merge_delta_path(
            spark,
            source,
            str(tmp_path / "duplicate-target"),
            keys=("date", "serial_number"),
        )


def test_requires_at_least_one_merge_key(spark: SparkSession, tmp_path: Path) -> None:
    source = spark.createDataFrame([("SERIAL-A",)], ["serial_number"])

    with pytest.raises(ValueError, match="At least one merge key is required"):
        merge_delta_path(spark, source, str(tmp_path / "no-key-target"), keys=())
