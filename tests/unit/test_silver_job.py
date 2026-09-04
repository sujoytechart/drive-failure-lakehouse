from datetime import datetime

from pyspark.sql import SparkSession

from drive_failure_lakehouse.jobs.silver import select_publishable_frames
from drive_failure_lakehouse.silver import build_silver_records


def test_publishes_stable_silver_and_quarantine_columns(spark: SparkSession) -> None:
    source = spark.createDataFrame(
        [
            (
                "invalid-date",
                "SERIAL-A",
                "MODEL-A",
                "1000",
                "0",
                "bad-smart-value",
                "/landing/release=2024-Q1-r1/day.csv",
                "2024-Q1",
                1,
                datetime(2024, 4, 1, 12, 0),
                datetime(2024, 4, 1, 12, 1),
                "{}",
                "unexpected-source-column",
            )
        ],
        [
            "date",
            "serial_number",
            "model",
            "capacity_bytes",
            "failure",
            "smart_5_raw",
            "source_file",
            "source_quarter",
            "source_revision",
            "file_modification_time",
            "ingested_at",
            "_rescued_data",
            "future_column",
        ],
    )

    trusted, quarantine = select_publishable_frames(build_silver_records(source))

    assert "future_column" not in trusted.columns
    assert "future_column" not in quarantine.columns
    assert quarantine.columns == [
        "quarantine_id",
        "source_file",
        "source_quarter",
        "source_revision",
        "file_modification_time",
        "ingested_at",
        "raw_record",
        "rejection_reasons",
    ]
    row = quarantine.first()
    assert row is not None
    assert "unexpected-source-column" in row.raw_record
