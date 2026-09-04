from datetime import datetime

from pyspark.sql import DataFrame, SparkSession

from drive_failure_lakehouse.silver import UNRESOLVED_DUPLICATE, build_silver_records

COLUMNS = [
    "date",
    "serial_number",
    "model",
    "capacity_bytes",
    "failure",
    "source_file",
    "source_revision",
    "file_modification_time",
]


def source_frame(spark: SparkSession, rows: list[tuple[object, ...]]) -> DataFrame:
    return spark.createDataFrame(rows, COLUMNS)


def test_later_source_revision_replaces_earlier_record(spark: SparkSession) -> None:
    observed_at = datetime(2024, 4, 1, 12, 0)
    source = source_frame(
        spark,
        [
            ("2024-01-02", "SERIAL-A", "MODEL-A", "1000", "0", "r1.csv", 1, observed_at),
            ("2024-01-02", "SERIAL-A", "MODEL-A", "1000", "1", "r2.csv", 2, observed_at),
        ],
    )

    frames = build_silver_records(source)
    row = frames.trusted.first()

    assert row is not None
    assert frames.trusted.count() == 1
    assert frames.rejected.count() == 0
    assert row.failure == 1
    assert row.source_revision == 2


def test_newer_file_modification_time_wins_within_revision(spark: SparkSession) -> None:
    source = source_frame(
        spark,
        [
            (
                "2024-01-02",
                "SERIAL-A",
                "MODEL-A",
                "1000",
                "0",
                "older.csv",
                1,
                datetime(2024, 4, 1, 12, 0),
            ),
            (
                "2024-01-02",
                "SERIAL-A",
                "MODEL-A",
                "1000",
                "1",
                "newer.csv",
                1,
                datetime(2024, 4, 2, 12, 0),
            ),
        ],
    )

    row = build_silver_records(source).trusted.first()

    assert row is not None
    assert row.failure == 1
    assert row.source_file == "newer.csv"


def test_source_filename_breaks_equal_revision_and_time_tie(spark: SparkSession) -> None:
    observed_at = datetime(2024, 4, 1, 12, 0)
    source = source_frame(
        spark,
        [
            ("2024-01-02", "SERIAL-A", "MODEL-A", "1000", "0", "a.csv", 1, observed_at),
            ("2024-01-02", "SERIAL-A", "MODEL-A", "1000", "1", "z.csv", 1, observed_at),
        ],
    )

    row = build_silver_records(source).trusted.first()

    assert row is not None
    assert row.failure == 1
    assert row.source_file == "z.csv"


def test_conflicting_top_precedence_rows_are_quarantined(spark: SparkSession) -> None:
    observed_at = datetime(2024, 4, 1, 12, 0)
    source = source_frame(
        spark,
        [
            ("2024-01-02", "SERIAL-A", "MODEL-A", "1000", "0", "same.csv", 1, observed_at),
            ("2024-01-02", "SERIAL-A", "MODEL-A", "1000", "1", "same.csv", 1, observed_at),
        ],
    )

    frames = build_silver_records(source)
    rejected = frames.rejected.collect()

    assert frames.trusted.count() == 0
    assert len(rejected) == 2
    assert all(row.rejection_reasons == [UNRESOLVED_DUPLICATE] for row in rejected)
    assert len({row.quarantine_id for row in rejected}) == 2
