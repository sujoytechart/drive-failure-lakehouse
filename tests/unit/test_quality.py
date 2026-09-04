import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, IntegerType, LongType

from drive_failure_lakehouse.quality import (
    INVALID_CAPACITY_BYTES,
    INVALID_DATE,
    INVALID_FAILURE_FLAG,
    MISSING_SERIAL_NUMBER,
    validate_drive_rows,
)


def test_separates_typed_valid_row_from_rejected_rows(spark: SparkSession) -> None:
    source = spark.createDataFrame(
        [
            ("2024-01-02", "SERIAL-A", "MODEL-A", "1000", "0", "valid.csv", 1),
            ("not-a-date", "SERIAL-B", "MODEL-A", "1000", "0", "bad-date.csv", 1),
            ("2024-01-02", "  ", "MODEL-A", "1000", "0", "blank-serial.csv", 1),
            ("2024-01-02", "SERIAL-C", "MODEL-A", "1000", "2", "bad-flag.csv", 1),
            ("2024-01-02", "SERIAL-D", "MODEL-A", "unknown", "0", "bad-cap.csv", 1),
            ("2024-01-02", "SERIAL-E", "MODEL-A", "0", "0", "zero-cap.csv", 1),
        ],
        [
            "date",
            "serial_number",
            "model",
            "capacity_bytes",
            "failure",
            "source_file",
            "source_revision",
        ],
    )

    frames = validate_drive_rows(source)

    assert frames.accepted.count() == 1
    assert isinstance(frames.accepted.schema["date"].dataType, DateType)
    assert isinstance(frames.accepted.schema["capacity_bytes"].dataType, LongType)
    assert isinstance(frames.accepted.schema["failure"].dataType, IntegerType)

    rejected = {row.source_file: row for row in frames.rejected.collect()}
    assert len(rejected) == 5
    assert rejected["bad-date.csv"].rejection_reasons == [INVALID_DATE]
    assert rejected["blank-serial.csv"].rejection_reasons == [MISSING_SERIAL_NUMBER]
    assert rejected["bad-flag.csv"].rejection_reasons == [INVALID_FAILURE_FLAG]
    assert rejected["bad-cap.csv"].rejection_reasons == [INVALID_CAPACITY_BYTES]
    assert rejected["zero-cap.csv"].rejection_reasons == [INVALID_CAPACITY_BYTES]
    assert all(row.raw_record for row in rejected.values())
    assert all(len(row.quarantine_id) == 64 for row in rejected.values())


def test_quarantine_identifier_is_deterministic(spark: SparkSession) -> None:
    source = spark.createDataFrame(
        [("invalid", "SERIAL-A", "MODEL-A", "1000", "0", "source.csv", 2)],
        [
            "date",
            "serial_number",
            "model",
            "capacity_bytes",
            "failure",
            "source_file",
            "source_revision",
        ],
    )

    first = validate_drive_rows(source).rejected.first()
    second = validate_drive_rows(source.select(*reversed(source.columns))).rejected.first()

    assert first is not None
    assert second is not None
    assert first.rejection_reasons == [INVALID_DATE]
    assert first.raw_record == second.raw_record
    assert first.quarantine_id == second.quarantine_id


def test_rejects_dataframe_missing_required_columns(spark: SparkSession) -> None:
    source = spark.createDataFrame([("SERIAL-A",)], ["serial_number"])

    with pytest.raises(ValueError, match="Missing required columns"):
        validate_drive_rows(source)
