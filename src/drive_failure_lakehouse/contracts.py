"""Shared Spark schemas for stable lakehouse layer boundaries."""

from typing import Final

from pyspark.sql.types import ArrayType, LongType, MapType, StringType, StructField, StructType

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
