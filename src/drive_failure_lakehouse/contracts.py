"""Shared Spark schemas for stable lakehouse layer boundaries."""

from typing import Final

from pyspark.sql.types import (
    ArrayType,
    DateType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

SMART_READING_TYPE: Final = StructType(
    [
        StructField("raw", LongType(), nullable=True),
        StructField("normalized", LongType(), nullable=True),
    ]
)

SMART_ATTRIBUTES_TYPE: Final = MapType(
    StringType(),
    SMART_READING_TYPE,
    valueContainsNull=False,
)

QUALITY_WARNINGS_TYPE: Final = ArrayType(StringType(), containsNull=False)

GOLD_DAILY_SCHEMA: Final = StructType(
    [
        StructField("date", DateType(), nullable=True),
        StructField("model", StringType(), nullable=True),
        StructField("active_drives", LongType(), nullable=False),
        StructField("failures", LongType(), nullable=True),
        StructField("drive_days", LongType(), nullable=False),
    ]
)

GOLD_PERIOD_SCHEMA: Final = StructType(
    [
        StructField("period_start", DateType(), nullable=False),
        StructField("period_end", DateType(), nullable=False),
        StructField("model", StringType(), nullable=True),
        StructField("unique_drives_observed", LongType(), nullable=False),
        StructField("failed_drives", LongType(), nullable=False),
        StructField("total_drive_days", LongType(), nullable=False),
        StructField("naive_failed_drive_percentage", DoubleType(), nullable=True),
        StructField("failures_per_million_drive_days", DoubleType(), nullable=True),
    ]
)
