"""Build unique trusted Silver records with deterministic conflict handling."""

from dataclasses import dataclass
from typing import Final

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from drive_failure_lakehouse.quality import validate_drive_rows
from drive_failure_lakehouse.smart import with_smart_attributes

UNRESOLVED_DUPLICATE: Final = "UNRESOLVED_DUPLICATE"
BUSINESS_KEYS: Final = ("date", "serial_number")
PRECEDENCE_COLUMNS: Final = frozenset({"source_revision", "file_modification_time", "source_file"})


@dataclass(frozen=True, slots=True)
class SilverFrames:
    """Unique trusted drive-day records and all rejected candidates."""

    trusted: DataFrame
    rejected: DataFrame


def build_silver_records(bronze: DataFrame) -> SilverFrames:
    """Validate, normalize, and deterministically resolve Bronze drive-day rows.

    The highest source revision wins for each drive-day key, followed by the newest
    file modification time and lexically greatest source filename. Candidates that
    share the complete top precedence but disagree on business content are rejected.
    """
    if "file_modification_time" not in bronze.columns:
        raise ValueError("Missing required columns: file_modification_time")

    validated = validate_drive_rows(bronze)
    normalized = with_smart_attributes(validated.accepted)
    ranked = _rank_candidates(normalized)

    top_candidates = ranked.filter(F.col("_precedence_rank") == 1)
    conflict_keys = (
        top_candidates.groupBy(*BUSINESS_KEYS)
        .agg(F.countDistinct("_business_hash").alias("_business_versions"))
        .filter(F.col("_business_versions") > 1)
        .select(*BUSINESS_KEYS)
    )

    conflict_candidates = top_candidates.join(conflict_keys, list(BUSINESS_KEYS), "inner")
    trusted_candidates = top_candidates.join(conflict_keys, list(BUSINESS_KEYS), "left_anti")

    trusted = _select_one_identical_candidate(trusted_candidates)
    conflict_rejections = _build_conflict_rejections(conflict_candidates)
    rejected = validated.rejected.unionByName(conflict_rejections, allowMissingColumns=True)

    duplicate_key = (
        trusted.groupBy(*BUSINESS_KEYS).count().filter(F.col("count") > 1).limit(1).count()
    )
    if duplicate_key:
        raise RuntimeError("Silver trusted records contain duplicate drive-day keys")

    return SilverFrames(trusted=trusted, rejected=rejected)


def _rank_candidates(dataframe: DataFrame) -> DataFrame:
    business_columns = sorted(
        column_name for column_name in dataframe.columns if column_name not in PRECEDENCE_COLUMNS
    )
    business_json = F.to_json(
        F.struct(*(F.col(name).alias(name) for name in business_columns)),
        options={"ignoreNullFields": "false"},
    )
    precedence = Window.partitionBy(*BUSINESS_KEYS).orderBy(
        F.col("source_revision").desc_nulls_last(),
        F.col("file_modification_time").desc_nulls_last(),
        F.col("source_file").desc_nulls_last(),
    )
    return dataframe.withColumn("_business_hash", F.sha2(business_json, 256)).withColumn(
        "_precedence_rank", F.dense_rank().over(precedence)
    )


def _select_one_identical_candidate(dataframe: DataFrame) -> DataFrame:
    identical_candidate_order = Window.partitionBy(*BUSINESS_KEYS).orderBy(
        F.col("_business_hash"),
        F.col("source_file"),
    )
    return (
        dataframe.withColumn("_candidate_number", F.row_number().over(identical_candidate_order))
        .filter(F.col("_candidate_number") == 1)
        .drop("_candidate_number", "_business_hash", "_precedence_rank")
    )


def _build_conflict_rejections(dataframe: DataFrame) -> DataFrame:
    public_columns = sorted(
        column_name for column_name in dataframe.columns if not column_name.startswith("_")
    )
    raw_record = F.to_json(
        F.struct(*(F.col(name).alias(name) for name in public_columns)),
        options={"ignoreNullFields": "false"},
    )
    return (
        dataframe.withColumn("raw_record", raw_record)
        .withColumn("rejection_reasons", F.array(F.lit(UNRESOLVED_DUPLICATE)))
        .withColumn(
            "quarantine_id",
            F.sha2(
                F.concat_ws(
                    "\u001f",
                    F.coalesce(F.col("source_file").cast("string"), F.lit("<null>")),
                    F.coalesce(F.col("source_revision").cast("string"), F.lit("<null>")),
                    F.col("raw_record"),
                ),
                256,
            ),
        )
        .drop("_business_hash", "_precedence_rank")
    )
