"""Validated configuration shared by Databricks job entry points."""

import argparse
import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from typing import Final

from pyspark.sql import SparkSession

IDENTIFIER_PATTERN: Final = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
RELEASE_PATH_PATTERN: Final = re.compile(
    r"(?:^|/)release=(?P<quarter>\d{4}-Q[1-4])-r(?P<revision>[1-9]\d*)(?:/|$)"
)


@dataclass(frozen=True, slots=True)
class SourceRelease:
    """Parsed source quarter and monotonically increasing release revision."""

    source_quarter: str
    source_revision: int


@dataclass(frozen=True, slots=True)
class TableNames:
    """Fully qualified managed tables used by the medallion pipeline."""

    catalog: str
    bronze_schema: str
    silver_schema: str
    gold_schema: str

    def __post_init__(self) -> None:
        for value in (
            self.catalog,
            self.bronze_schema,
            self.silver_schema,
            self.gold_schema,
        ):
            validate_identifier(value)

    @property
    def bronze_raw(self) -> str:
        return f"{self.catalog}.{self.bronze_schema}.drive_telemetry_raw"

    @property
    def silver_drive_daily(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.drive_daily"

    @property
    def silver_quarantine(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.drive_daily_quarantine"

    @property
    def gold_daily(self) -> str:
        return f"{self.catalog}.{self.gold_schema}.drive_model_daily"

    @property
    def gold_period(self) -> str:
        return f"{self.catalog}.{self.gold_schema}.drive_model_period"


@dataclass(frozen=True, slots=True)
class JobConfig:
    """Validated resource names and reporting window supplied to every job."""

    catalog: str
    bronze_schema: str
    silver_schema: str
    gold_schema: str
    volume: str
    period_start: date
    period_end: date

    def __post_init__(self) -> None:
        for value in (
            self.catalog,
            self.bronze_schema,
            self.silver_schema,
            self.gold_schema,
            self.volume,
        ):
            validate_identifier(value)
        if self.period_start > self.period_end:
            raise ValueError("period_start must be on or before period_end")

    @property
    def tables(self) -> TableNames:
        return TableNames(
            catalog=self.catalog,
            bronze_schema=self.bronze_schema,
            silver_schema=self.silver_schema,
            gold_schema=self.gold_schema,
        )

    @property
    def volume_root(self) -> str:
        return f"/Volumes/{self.catalog}/{self.bronze_schema}/{self.volume}"

    @property
    def landing_path(self) -> str:
        return f"{self.volume_root}/landing"

    @property
    def schema_path(self) -> str:
        return f"{self.volume_root}/autoloader/schema"

    @property
    def checkpoint_path(self) -> str:
        return f"{self.volume_root}/autoloader/checkpoints/bronze"


def parse_release_path(path: str) -> SourceRelease:
    """Parse one ``release=YYYY-QN-rN`` directory from a source path."""
    matches = list(RELEASE_PATH_PATTERN.finditer(path))
    if len(matches) != 1:
        raise ValueError(f"Source path must contain one release=YYYY-QN-rN directory: {path}")
    match = matches[0]
    return SourceRelease(
        source_quarter=match.group("quarter"),
        source_revision=int(match.group("revision")),
    )


def validate_identifier(identifier: str) -> str:
    """Return a simple SQL identifier or reject punctuation requiring quoting."""
    if IDENTIFIER_PATTERN.fullmatch(identifier) is None:
        raise ValueError(f"Invalid SQL identifier: {identifier!r}")
    return identifier


def parse_job_config(arguments: Sequence[str] | None = None) -> JobConfig:
    """Parse the common command-line contract used by Databricks wheel tasks."""
    parser = argparse.ArgumentParser(description="Run a drive failure lakehouse task")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--bronze-schema", required=True)
    parser.add_argument("--silver-schema", required=True)
    parser.add_argument("--gold-schema", required=True)
    parser.add_argument("--volume", required=True)
    parser.add_argument("--period-start", required=True, type=date.fromisoformat)
    parser.add_argument("--period-end", required=True, type=date.fromisoformat)
    parsed = parser.parse_args(arguments)
    return JobConfig(
        catalog=parsed.catalog,
        bronze_schema=parsed.bronze_schema,
        silver_schema=parsed.silver_schema,
        gold_schema=parsed.gold_schema,
        volume=parsed.volume,
        period_start=parsed.period_start,
        period_end=parsed.period_end,
    )


def active_spark_session() -> SparkSession:
    """Return the Spark session supplied by Databricks or fail clearly."""
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active Spark session is available")
    return spark


def prepare_managed_resources(spark: SparkSession, config: JobConfig) -> None:
    """Create the schemas and managed landing volume required by the pipeline."""
    for schema in (config.bronze_schema, config.silver_schema, config.gold_schema):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.catalog}.{schema}")
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS {config.catalog}.{config.bronze_schema}.{config.volume}"
    )


def configure_logging() -> None:
    """Use a compact log format shared by all wheel-task entry points."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
