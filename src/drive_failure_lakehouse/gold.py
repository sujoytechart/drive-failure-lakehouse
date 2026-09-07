"""Build consumption-ready drive reliability metrics from trusted Silver rows."""

from datetime import date
from typing import Final

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from drive_failure_lakehouse.contracts import GOLD_DAILY_SCHEMA, GOLD_PERIOD_SCHEMA

REQUIRED_SILVER_COLUMNS: Final = frozenset({"date", "serial_number", "model", "failure"})


def build_daily_metrics(silver: DataFrame) -> DataFrame:
    """Return additive daily drive counts and failures at ``(date, model)`` grain."""
    _require_silver_columns(silver)
    daily = (
        silver.groupBy("date", "model")
        .agg(
            F.countDistinct("serial_number").alias("active_drives"),
            F.sum(F.col("failure").cast("long")).alias("failures"),
        )
        .withColumn("drive_days", F.col("active_drives"))
    )
    return daily.select(*GOLD_DAILY_SCHEMA.fieldNames())


def build_period_metrics(
    silver: DataFrame,
    period_start: date,
    period_end: date,
) -> DataFrame:
    """Return exposure-aware drive-model metrics for an inclusive date window.

    ``failures_per_million_drive_days`` is an incidence rate based on observed
    drive-days. It is not a survival probability and does not correct for censoring.
    """
    if period_start > period_end:
        raise ValueError("period_start must be on or before period_end")
    _require_silver_columns(silver)

    period_rows = silver.filter(F.col("date").between(F.lit(period_start), F.lit(period_end)))
    counts = period_rows.groupBy("model").agg(
        F.countDistinct("serial_number").alias("unique_drives_observed"),
        F.countDistinct(F.when(F.col("failure") == 1, F.col("serial_number"))).alias(
            "failed_drives"
        ),
        F.count(F.lit(1)).alias("total_drive_days"),
    )

    metrics = (
        counts.withColumn("period_start", F.lit(period_start).cast("date"))
        .withColumn("period_end", F.lit(period_end).cast("date"))
        .withColumn(
            "naive_failed_drive_percentage",
            F.col("failed_drives").cast("double")
            / F.col("unique_drives_observed").cast("double")
            * F.lit(100.0),
        )
        .withColumn(
            "failures_per_million_drive_days",
            F.col("failed_drives").cast("double")
            / F.col("total_drive_days").cast("double")
            * F.lit(1_000_000.0),
        )
    )
    return metrics.select(*GOLD_PERIOD_SCHEMA.fieldNames())


def _require_silver_columns(dataframe: DataFrame) -> None:
    missing_columns = sorted(REQUIRED_SILVER_COLUMNS.difference(dataframe.columns))
    if missing_columns:
        raise ValueError(f"Missing required Silver columns: {', '.join(missing_columns)}")
