from datetime import date

import pytest
from pyspark.sql import DataFrame, SparkSession

from drive_failure_lakehouse.contracts import GOLD_PERIOD_SCHEMA
from drive_failure_lakehouse.gold import build_daily_metrics, build_period_metrics


def silver_rows(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [
            (date(2024, 1, 1), "SERIAL-A", "MODEL-A", 0),
            (date(2024, 1, 1), "SERIAL-B", "MODEL-A", 0),
            (date(2024, 1, 1), "SERIAL-C", "MODEL-A", 1),
            (date(2024, 1, 2), "SERIAL-A", "MODEL-A", 0),
            (date(2024, 1, 2), "SERIAL-B", "MODEL-A", 0),
        ],
        ["date", "serial_number", "model", "failure"],
    )


def test_builds_hand_calculated_daily_metrics(spark: SparkSession) -> None:
    rows = {
        row.date: row for row in build_daily_metrics(silver_rows(spark)).orderBy("date").collect()
    }

    day_one = rows[date(2024, 1, 1)]
    day_two = rows[date(2024, 1, 2)]
    assert (day_one.active_drives, day_one.failures, day_one.drive_days) == (3, 1, 3)
    assert (day_two.active_drives, day_two.failures, day_two.drive_days) == (2, 0, 2)


def test_builds_hand_calculated_period_metrics(spark: SparkSession) -> None:
    row = build_period_metrics(
        silver_rows(spark),
        period_start=date(2024, 1, 1),
        period_end=date(2024, 1, 2),
    ).first()

    assert row is not None
    assert row.period_start == date(2024, 1, 1)
    assert row.period_end == date(2024, 1, 2)
    assert row.unique_drives_observed == 3
    assert row.failed_drives == 1
    assert row.total_drive_days == 5
    assert row.naive_failed_drive_percentage == pytest.approx(33.333333, rel=1e-6)
    assert row.failures_per_million_drive_days == pytest.approx(200_000.0)


def test_empty_period_has_declared_schema(spark: SparkSession) -> None:
    result = build_period_metrics(
        silver_rows(spark),
        period_start=date(2025, 1, 1),
        period_end=date(2025, 1, 31),
    )

    assert result.count() == 0
    assert result.schema == GOLD_PERIOD_SCHEMA


def test_rejects_reversed_reporting_window(spark: SparkSession) -> None:
    with pytest.raises(ValueError, match="period_start must be on or before period_end"):
        build_period_metrics(
            silver_rows(spark),
            period_start=date(2024, 2, 1),
            period_end=date(2024, 1, 1),
        )
