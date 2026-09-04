from pyspark.sql import SparkSession

from drive_failure_lakehouse.jobs.smoke import validate_platform


def test_platform_smoke_executes_sql_in_active_catalog(spark: SparkSession) -> None:
    context = validate_platform(spark)

    assert context.platform_ok == 1
    assert context.catalog
    assert context.schema
