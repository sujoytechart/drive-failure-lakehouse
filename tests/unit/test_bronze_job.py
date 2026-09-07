from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from drive_failure_lakehouse.jobs.bronze import with_release_metadata


def test_extracts_release_metadata_from_source_file(spark: SparkSession) -> None:
    source = spark.createDataFrame(
        [
            (
                "/Volumes/workspace/bronze/files/landing/release=2024-Q1-r2/day.csv",
                datetime(2024, 4, 1, 12, 0),
            )
        ],
        ["source_file", "file_modification_time"],
    )

    row = with_release_metadata(source).first()

    assert row is not None
    assert row.source_quarter == "2024-Q1"
    assert row.source_revision == 2
    assert row.ingested_at is not None


def test_fails_when_source_path_has_no_valid_release(spark: SparkSession) -> None:
    source = spark.createDataFrame(
        [("/Volumes/workspace/bronze/files/landing/day.csv",)],
        ["source_file"],
    )

    with pytest.raises(Exception, match="release=YYYY-QN-rN"):
        with_release_metadata(source).collect()
