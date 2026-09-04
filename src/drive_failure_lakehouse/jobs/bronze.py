"""Incrementally ingest released Backblaze CSV files into Bronze."""

import logging
from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from drive_failure_lakehouse.jobs.common import (
    JobConfig,
    active_spark_session,
    configure_logging,
    parse_job_config,
)

LOGGER = logging.getLogger(__name__)
RELEASE_PATTERN = r"(?:^|/)release=(\d{4}-Q[1-4])-r([1-9]\d*)(?:/|$)"


def with_release_metadata(dataframe: DataFrame) -> DataFrame:
    """Add release lineage and fail when a file is outside the landing contract."""
    source_file = F.col("source_file")
    source_quarter = F.regexp_extract(source_file, RELEASE_PATTERN, 1)
    source_revision = F.regexp_extract(source_file, RELEASE_PATTERN, 2)
    invalid_path_message = F.concat(
        F.lit("Source path must contain release=YYYY-QN-rN: "),
        F.coalesce(source_file.cast("string"), F.lit("<null>")),
    )
    return (
        dataframe.withColumn(
            "source_quarter",
            F.when(source_quarter != "", source_quarter).otherwise(
                F.raise_error(invalid_path_message)
            ),
        )
        .withColumn(
            "source_revision",
            F.when(source_revision != "", source_revision.cast("int")).otherwise(
                F.raise_error(invalid_path_message).cast("int")
            ),
        )
        .withColumn("ingested_at", F.current_timestamp())
    )


def build_bronze_stream(spark: SparkSession, config: JobConfig) -> DataFrame:
    """Build the Auto Loader stream while retaining source-file metadata."""
    loaded = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", config.schema_path)
        .option("cloudFiles.inferColumnTypes", "false")
        .option("rescuedDataColumn", "_rescued_data")
        .option("pathGlobFilter", "*.csv")
        .option("header", "true")
        .load(config.landing_path)
    )
    with_file_metadata = loaded.select(
        "*",
        F.col("_metadata.file_path").alias("source_file"),
        F.col("_metadata.file_modification_time").alias("file_modification_time"),
    )
    return with_release_metadata(with_file_metadata)


def run_bronze(spark: SparkSession, config: JobConfig) -> None:
    """Process all newly discovered landing files and stop when caught up."""
    query = (
        build_bronze_stream(spark, config)
        .writeStream.format("delta")
        .option("checkpointLocation", config.checkpoint_path)
        .outputMode("append")
        .trigger(availableNow=True)
        .queryName("drive_failure_bronze_ingestion")
        .toTable(config.tables.bronze_raw)
    )
    query.awaitTermination()
    progress = query.lastProgress
    LOGGER.info(
        "bronze_complete table=%s progress=%s",
        config.tables.bronze_raw,
        progress if progress is not None else "none",
    )


def main(arguments: Sequence[str] | None = None) -> None:
    """Run Bronze using arguments supplied by the Databricks wheel task."""
    configure_logging()
    run_bronze(active_spark_session(), parse_job_config(arguments))
