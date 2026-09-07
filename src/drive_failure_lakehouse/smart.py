"""Normalize evolving wide SMART telemetry into a stable Spark map."""

import re
from collections.abc import Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from drive_failure_lakehouse.contracts import SMART_ATTRIBUTES_TYPE

SMART_COLUMN_PATTERN = re.compile(r"^smart_(\d+)_(raw|normalized)$")


def discover_smart_attributes(columns: Sequence[str]) -> tuple[str, ...]:
    """Return numerically sorted SMART attribute identifiers found in columns."""
    identifiers = {
        match.group(1)
        for column_name in columns
        if (match := SMART_COLUMN_PATTERN.fullmatch(column_name)) is not None
    }
    return tuple(sorted(identifiers, key=int))


def with_smart_attributes(dataframe: DataFrame) -> DataFrame:
    """Replace wide SMART columns with a stable map and conversion warnings.

    Non-empty values that cannot be converted to integers become null readings and
    add an ``INVALID_SMART_<id>_<kind>`` warning. An attribute is omitted from the
    map when both its raw and normalized readings are null.
    """
    attributes = discover_smart_attributes(dataframe.columns)
    smart_column_names = [
        column_name
        for column_name in dataframe.columns
        if SMART_COLUMN_PATTERN.fullmatch(column_name) is not None
    ]

    map_arguments: list[Column] = []
    warning_columns: list[Column] = []

    for attribute in attributes:
        raw_value, raw_warning = _reading_and_warning(dataframe, attribute, "raw")
        normalized_value, normalized_warning = _reading_and_warning(
            dataframe,
            attribute,
            "normalized",
        )
        map_arguments.extend(
            [
                F.lit(attribute),
                F.struct(
                    raw_value.alias("raw"),
                    normalized_value.alias("normalized"),
                ),
            ]
        )
        warning_columns.extend([raw_warning, normalized_warning])

    smart_attributes = _build_smart_map(map_arguments)
    new_warnings = _compact_warnings(warning_columns)

    if "quality_warnings" in dataframe.columns:
        empty_warnings = F.expr("cast(array() as array<string>)")
        quality_warnings = F.concat(
            F.coalesce(F.col("quality_warnings"), empty_warnings),
            new_warnings,
        )
    else:
        quality_warnings = new_warnings

    return (
        dataframe.withColumn("smart_attributes", smart_attributes)
        .withColumn("quality_warnings", quality_warnings)
        .drop(*smart_column_names)
    )


def _reading_and_warning(
    dataframe: DataFrame,
    attribute: str,
    reading_kind: str,
) -> tuple[Column, Column]:
    column_name = f"smart_{attribute}_{reading_kind}"
    if column_name not in dataframe.columns:
        return F.lit(None).cast("bigint"), F.lit(None).cast("string")

    source_value = F.col(column_name)
    numeric_value = F.expr(f"try_cast(`{column_name}` as bigint)")
    has_source_value = source_value.isNotNull() & (F.trim(source_value.cast("string")) != "")
    warning_code = f"INVALID_SMART_{attribute}_{reading_kind.upper()}"
    warning = F.when(has_source_value & numeric_value.isNull(), F.lit(warning_code))
    return numeric_value, warning


def _build_smart_map(map_arguments: list[Column]) -> Column:
    if not map_arguments:
        return F.expr("cast(map() as map<string, struct<raw:bigint, normalized:bigint>>)")

    readings = F.create_map(*map_arguments)
    populated_readings = F.map_filter(
        readings,
        lambda _key, reading: reading["raw"].isNotNull() | reading["normalized"].isNotNull(),
    )
    return populated_readings.cast(SMART_ATTRIBUTES_TYPE)


def _compact_warnings(warning_columns: list[Column]) -> Column:
    if not warning_columns:
        return F.expr("cast(array() as array<string>)")
    return F.filter(F.array(*warning_columns), lambda warning: warning.isNotNull())
