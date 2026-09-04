from datetime import date

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from drive_failure_lakehouse.gold import build_daily_metrics, build_period_metrics
from drive_failure_lakehouse.jobs.gold import validate_gold_metrics


def test_accepts_consistent_gold_metrics(spark: SparkSession) -> None:
    silver = spark.createDataFrame(
        [(date(2024, 1, 1), "S1", "Model A", 0)],
        ["date", "serial_number", "model", "failure"],
    )
    daily = build_daily_metrics(silver)
    period = build_period_metrics(silver, date(2024, 1, 1), date(2024, 1, 31))

    validate_gold_metrics(daily, period)


def test_rejects_more_daily_failures_than_active_drives(spark: SparkSession) -> None:
    silver = spark.createDataFrame(
        [(date(2024, 1, 1), "S1", "Model A", 0)],
        ["date", "serial_number", "model", "failure"],
    )
    invalid_daily = build_daily_metrics(silver).withColumn(
        "failures", F.col("active_drives") + F.lit(1)
    )
    period = build_period_metrics(silver, date(2024, 1, 1), date(2024, 1, 31))

    with pytest.raises(ValueError, match="impossible counts"):
        validate_gold_metrics(invalid_daily, period)


def test_rejects_null_metric_counts(spark: SparkSession) -> None:
    silver = spark.createDataFrame(
        [(date(2024, 1, 1), "S1", "Model A", 0)],
        ["date", "serial_number", "model", "failure"],
    )
    invalid_daily = build_daily_metrics(silver).withColumn("failures", F.lit(None).cast("long"))
    period = build_period_metrics(silver, date(2024, 1, 1), date(2024, 1, 31))

    with pytest.raises(ValueError, match="impossible counts"):
        validate_gold_metrics(invalid_daily, period)
