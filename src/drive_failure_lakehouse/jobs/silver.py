"""Publish trusted drive-day records and durable quarantine evidence."""

import logging
from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession

from drive_failure_lakehouse.delta_io import merge_delta_table
from drive_failure_lakehouse.jobs.common import (
    JobConfig,
    active_spark_session,
    configure_logging,
    parse_job_config,
    prepare_managed_resources,
)
from drive_failure_lakehouse.silver import BUSINESS_KEYS, SilverFrames, build_silver_records

LOGGER = logging.getLogger(__name__)
TRUSTED_COLUMNS = (
    "date",
    "serial_number",
    "model",
    "capacity_bytes",
    "failure",
    "smart_attributes",
    "quality_warnings",
    "source_file",
    "source_quarter",
    "source_revision",
    "file_modification_time",
    "ingested_at",
    "_rescued_data",
)
QUARANTINE_COLUMNS = (
    "quarantine_id",
    "source_file",
    "source_quarter",
    "source_revision",
    "file_modification_time",
    "ingested_at",
    "raw_record",
    "rejection_reasons",
)


def select_publishable_frames(frames: SilverFrames) -> tuple[DataFrame, DataFrame]:
    """Project stable table contracts while raw JSON retains rejected source fields."""
    return (
        frames.trusted.select(*TRUSTED_COLUMNS),
        frames.rejected.select(*QUARANTINE_COLUMNS),
    )


def run_silver(spark: SparkSession, config: JobConfig) -> None:
    """Transform Bronze, upsert trusted rows, and upsert rejected evidence."""
    prepare_managed_resources(spark, config)
    bronze = spark.table(config.tables.bronze_raw)
    bronze_count = bronze.count()
    trusted, quarantine = select_publishable_frames(build_silver_records(bronze))
    trusted_count = trusted.count()
    rejected_count = quarantine.count()

    merge_delta_table(
        spark,
        trusted,
        config.tables.silver_drive_daily,
        BUSINESS_KEYS,
    )
    merge_delta_table(
        spark,
        quarantine,
        config.tables.silver_quarantine,
        ("quarantine_id",),
    )
    LOGGER.info(
        "silver_complete bronze_rows=%d trusted_candidates=%d rejected_candidates=%d "
        "trusted_table=%s quarantine_table=%s",
        bronze_count,
        trusted_count,
        rejected_count,
        config.tables.silver_drive_daily,
        config.tables.silver_quarantine,
    )


def main(arguments: Sequence[str] | None = None) -> None:
    """Run Silver using arguments supplied by the Databricks wheel task."""
    configure_logging()
    run_silver(active_spark_session(), parse_job_config(arguments))
