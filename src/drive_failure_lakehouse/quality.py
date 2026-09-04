"""Validate Bronze drive-day records and preserve rejected source evidence."""

from dataclasses import dataclass
from typing import Final

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

INVALID_DATE: Final = "INVALID_DATE"
MISSING_SERIAL_NUMBER: Final = "MISSING_SERIAL_NUMBER"
INVALID_FAILURE_FLAG: Final = "INVALID_FAILURE_FLAG"
INVALID_CAPACITY_BYTES: Final = "INVALID_CAPACITY_BYTES"

REQUIRED_COLUMNS: Final = frozenset(
    {
        "date",
        "serial_number",
        "capacity_bytes",
        "failure",
        "source_file",
        "source_revision",
    }
)


@dataclass(frozen=True, slots=True)
class ValidationFrames:
    """Accepted typed records and rejected records with diagnostic evidence."""

    accepted: DataFrame
    rejected: DataFrame


def validate_drive_rows(dataframe: DataFrame) -> ValidationFrames:
    """Split Bronze rows into typed accepted rows and deterministic quarantine rows.

    A ``ValueError`` is raised when required source or lineage columns are absent.
    Invalid values remain in ``rejected`` with reason codes, the complete raw input
    serialized in stable column order, and a replay-stable quarantine identifier.
    """
    missing_columns = sorted(REQUIRED_COLUMNS.difference(dataframe.columns))
    if missing_columns:
        raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")

    original_columns = sorted(dataframe.columns)
    raw_record = F.to_json(
        F.struct(*(F.col(name).alias(name) for name in original_columns)),
        options={"ignoreNullFields": "false"},
    )

    parsed_date = F.expr("try_cast(`date` as date)")
    parsed_capacity = F.expr("try_cast(`capacity_bytes` as bigint)")
    parsed_failure = F.expr("try_cast(`failure` as int)")
    trimmed_serial = F.trim(F.col("serial_number"))

    with_typed_values = (
        dataframe.withColumn("raw_record", raw_record)
        .withColumn("date", parsed_date)
        .withColumn("serial_number", trimmed_serial)
        .withColumn("capacity_bytes", parsed_capacity)
        .withColumn("failure", parsed_failure)
    )

    rejection_reasons = _compact_reasons(
        [
            F.when(F.col("date").isNull(), F.lit(INVALID_DATE)),
            F.when(
                F.col("serial_number").isNull() | (F.col("serial_number") == ""),
                F.lit(MISSING_SERIAL_NUMBER),
            ),
            F.when(
                F.col("failure").isNull() | ~F.col("failure").isin(0, 1),
                F.lit(INVALID_FAILURE_FLAG),
            ),
            F.when(
                F.col("capacity_bytes").isNull() | (F.col("capacity_bytes") <= 0),
                F.lit(INVALID_CAPACITY_BYTES),
            ),
        ]
    )

    with_diagnostics = with_typed_values.withColumn(
        "rejection_reasons", rejection_reasons
    ).withColumn(
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

    accepted = with_diagnostics.filter(F.size("rejection_reasons") == 0).drop(
        "rejection_reasons",
        "raw_record",
        "quarantine_id",
    )
    rejected = with_diagnostics.filter(F.size("rejection_reasons") > 0)
    return ValidationFrames(accepted=accepted, rejected=rejected)


def _compact_reasons(reason_columns: list[Column]) -> Column:
    return F.filter(F.array(*reason_columns), lambda reason: reason.isNotNull())
