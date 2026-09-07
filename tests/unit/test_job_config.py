from datetime import date

import pytest

from drive_failure_lakehouse.jobs.common import (
    JobConfig,
    TableNames,
    parse_job_config,
    parse_release_path,
    validate_identifier,
)


def test_parses_release_metadata_from_landing_path() -> None:
    release = parse_release_path(
        "/Volumes/workspace/drive_failure_bronze/source/landing/release=2024-Q1-r2/day.csv"
    )

    assert release.source_quarter == "2024-Q1"
    assert release.source_revision == 2


@pytest.mark.parametrize(
    "path",
    [
        "/landing/2024-Q1/day.csv",
        "/landing/release=2024-Q5-r1/day.csv",
        "/landing/release=2024-Q1/day.csv",
        "/landing/release=2024-Q1-r0/day.csv",
    ],
)
def test_rejects_invalid_release_paths(path: str) -> None:
    with pytest.raises(ValueError, match="release=YYYY-QN-rN"):
        parse_release_path(path)


@pytest.mark.parametrize("identifier", ["workspace", "drive_failure_silver", "table_2"])
def test_accepts_simple_identifiers(identifier: str) -> None:
    assert validate_identifier(identifier) == identifier


@pytest.mark.parametrize(
    "identifier",
    ["catalog.schema", "bad-name", "bad name", "`quoted`", "table;drop", "2table", ""],
)
def test_rejects_unsafe_identifiers(identifier: str) -> None:
    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        validate_identifier(identifier)


def test_builds_table_names_from_validated_namespaces() -> None:
    tables = TableNames(
        catalog="workspace",
        bronze_schema="drive_failure_bronze",
        silver_schema="drive_failure_silver",
        gold_schema="drive_failure_gold",
    )

    assert tables.bronze_raw == "workspace.drive_failure_bronze.drive_telemetry_raw"
    assert tables.silver_drive_daily == "workspace.drive_failure_silver.drive_daily"
    assert tables.silver_quarantine == "workspace.drive_failure_silver.drive_daily_quarantine"
    assert tables.gold_daily == "workspace.drive_failure_gold.drive_model_daily"
    assert tables.gold_period == "workspace.drive_failure_gold.drive_model_period"


def test_builds_volume_paths_and_reporting_window_from_cli_arguments() -> None:
    config = parse_job_config(
        [
            "--catalog",
            "workspace",
            "--bronze-schema",
            "bronze",
            "--silver-schema",
            "silver",
            "--gold-schema",
            "gold",
            "--volume",
            "drive_files",
            "--period-start",
            "2024-01-01",
            "--period-end",
            "2024-03-31",
        ]
    )

    assert config.landing_path == "/Volumes/workspace/bronze/drive_files/landing"
    assert config.schema_path == "/Volumes/workspace/bronze/drive_files/autoloader/schema"
    assert config.checkpoint_path == (
        "/Volumes/workspace/bronze/drive_files/autoloader/checkpoints/bronze"
    )
    assert config.period_start == date(2024, 1, 1)
    assert config.period_end == date(2024, 3, 31)


def test_rejects_inverted_reporting_window() -> None:
    with pytest.raises(ValueError, match="period_start must be on or before period_end"):
        JobConfig(
            catalog="workspace",
            bronze_schema="bronze",
            silver_schema="silver",
            gold_schema="gold",
            volume="drive_files",
            period_start=date(2024, 4, 1),
            period_end=date(2024, 3, 31),
        )
