"""Minimal Databricks platform compatibility check."""

import logging
from dataclasses import dataclass

from pyspark.sql import SparkSession

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PlatformContext:
    """Catalog context returned by a successful Spark SQL smoke query."""

    catalog: str
    schema: str
    platform_ok: int


def validate_platform(spark: SparkSession) -> PlatformContext:
    """Execute a small SQL query and return its catalog context.

    A ``RuntimeError`` is raised if Spark does not return the expected sentinel.
    """
    row = spark.sql(
        "SELECT current_catalog() AS catalog, current_schema() AS schema, 1 AS platform_ok"
    ).first()
    if row is None or row.platform_ok != 1:
        raise RuntimeError("Databricks platform smoke query returned an invalid result")
    return PlatformContext(
        catalog=str(row.catalog),
        schema=str(row.schema),
        platform_ok=int(row.platform_ok),
    )


def main() -> None:
    """Run the smoke query using the Spark session supplied by Databricks."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active Spark session is available")

    context = validate_platform(spark)
    LOGGER.info(
        "platform_smoke catalog=%s schema=%s platform_ok=%d",
        context.catalog,
        context.schema,
        context.platform_ok,
    )
