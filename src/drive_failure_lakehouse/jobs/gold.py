"""Publish daily and reporting-period drive reliability tables."""

import logging
from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from drive_failure_lakehouse.delta_io import replace_delta_table
from drive_failure_lakehouse.gold import build_daily_metrics, build_period_metrics
from drive_failure_lakehouse.jobs.common import (
    JobConfig,
    active_spark_session,
    configure_logging,
    parse_job_config,
    prepare_managed_resources,
)

LOGGER = logging.getLogger(__name__)


def validate_gold_metrics(daily: DataFrame, period: DataFrame) -> None:
    """Reject impossible counts and invalid rate denominators before publishing."""
    invalid_daily = daily.filter(
        F.col("active_drives").isNull()
        | F.col("failures").isNull()
        | F.col("drive_days").isNull()
        | (F.col("active_drives") < 0)
        | (F.col("failures") < 0)
        | (F.col("drive_days") < 0)
        | (F.col("failures") > F.col("active_drives"))
    )
    invalid_period = period.filter(
        F.col("unique_drives_observed").isNull()
        | F.col("failed_drives").isNull()
        | F.col("total_drive_days").isNull()
        | F.col("naive_failed_drive_percentage").isNull()
        | F.col("failures_per_million_drive_days").isNull()
        | (F.col("unique_drives_observed") <= 0)
        | (F.col("failed_drives") < 0)
        | (F.col("total_drive_days") <= 0)
        | (F.col("failed_drives") > F.col("unique_drives_observed"))
    )
    if invalid_daily.limit(1).count() or invalid_period.limit(1).count():
        raise ValueError("Gold metrics contain impossible counts or invalid denominators")


def run_gold(spark: SparkSession, config: JobConfig) -> None:
    """Build and atomically replace both consumption-ready Gold tables."""
    prepare_managed_resources(spark, config)
    silver = spark.table(config.tables.silver_drive_daily)
    silver_count = silver.count()
    daily = build_daily_metrics(silver)
    period = build_period_metrics(silver, config.period_start, config.period_end)
    validate_gold_metrics(daily, period)
    daily_count = daily.count()
    period_count = period.count()

    replace_delta_table(daily, config.tables.gold_daily)
    replace_delta_table(period, config.tables.gold_period)
    LOGGER.info(
        "gold_complete silver_rows=%d daily_rows=%d period_rows=%d "
        "period_start=%s period_end=%s daily_table=%s period_table=%s",
        silver_count,
        daily_count,
        period_count,
        config.period_start,
        config.period_end,
        config.tables.gold_daily,
        config.tables.gold_period,
    )


def main(arguments: Sequence[str] | None = None) -> None:
    """Run Gold using arguments supplied by the Databricks wheel task."""
    configure_logging()
    run_gold(active_spark_session(), parse_job_config(arguments))
