"""Transactional Delta write boundaries used by local tests and Databricks jobs."""

import re
from collections.abc import Sequence
from typing import Final

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

MERGE_KEY_PATTERN: Final = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def merge_delta_path(
    spark: SparkSession,
    source: DataFrame,
    path: str,
    keys: Sequence[str],
) -> None:
    """Upsert unique source records into a Delta path using null-safe keys."""
    merge_keys = _validate_merge_source(source, keys)
    if not DeltaTable.isDeltaTable(spark, path):
        source.write.format("delta").mode("error").save(path)
        return

    target = DeltaTable.forPath(spark, path)
    _execute_merge(target, source, merge_keys)


def merge_delta_table(
    spark: SparkSession,
    source: DataFrame,
    table_name: str,
    keys: Sequence[str],
) -> None:
    """Upsert unique source records into a catalog-managed Delta table."""
    merge_keys = _validate_merge_source(source, keys)
    if not spark.catalog.tableExists(table_name):
        source.write.format("delta").mode("error").saveAsTable(table_name)
        return

    target = DeltaTable.forName(spark, table_name)
    _execute_merge(target, source, merge_keys)


def replace_delta_table(source: DataFrame, table_name: str) -> None:
    """Atomically replace a fully derived managed Delta table and its schema."""
    (
        source.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )


def _validate_merge_source(source: DataFrame, keys: Sequence[str]) -> tuple[str, ...]:
    merge_keys = tuple(keys)
    if not merge_keys:
        raise ValueError("At least one merge key is required")

    invalid_keys = [key for key in merge_keys if MERGE_KEY_PATTERN.fullmatch(key) is None]
    if invalid_keys:
        raise ValueError(f"Invalid merge keys: {', '.join(invalid_keys)}")

    missing_keys = [key for key in merge_keys if key not in source.columns]
    if missing_keys:
        raise ValueError(f"Merge keys missing from source: {', '.join(missing_keys)}")

    duplicate_exists = (
        source.groupBy(*merge_keys).count().filter(F.col("count") > 1).limit(1).count()
    )
    if duplicate_exists:
        raise ValueError(f"Source contains duplicate merge keys: {', '.join(merge_keys)}")
    return merge_keys


def _execute_merge(target: DeltaTable, source: DataFrame, keys: Sequence[str]) -> None:
    condition = " AND ".join(f"target.`{key}` <=> source.`{key}`" for key in keys)
    (
        target.alias("target")
        .merge(source.alias("source"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
