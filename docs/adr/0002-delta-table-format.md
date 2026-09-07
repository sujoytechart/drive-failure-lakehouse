# ADR 0002: Use Delta Lake as the table format

- Status: Accepted
- Date: 2026-09-03

## Context

The pipeline must preserve raw history, apply corrected source releases without
duplicating trusted records, quarantine invalid input, and publish derived tables
atomically. Plain object files do not provide the transaction log or row-level
operations needed for those guarantees.

Databricks Free Edition provides native Delta Lake integration on managed storage.
Local tests also need to exercise the same transaction semantics without requiring a
cloud workspace.

## Decision

Bronze, Silver, quarantine, and Gold datasets use Delta Lake tables. Bronze is
append-only through Auto Loader. Silver and quarantine use keyed `MERGE` operations.
Gold tables use atomic replacement because they are fully derived from Silver.

The core transformation functions remain Spark DataFrame functions. Delta is kept at
the persistence boundary so transformation tests do not depend on table storage.

## Alternatives considered

### Raw Parquet files

Parquet is the columnar file format underneath many analytical tables, including
Delta. A directory of Parquet files alone has no transaction log, atomic table
commit, schema history, or native merge operation. Implementing those concerns with
custom manifests would add failure modes without strengthening the required
reliability guarantees.

### Apache Iceberg

Iceberg also provides an open transactional table format and is a strong option for
engines or platforms standardized on its catalog model. Delta is selected because it
is the native Databricks path, its merge behavior is directly testable with
`delta-spark`, and this project does not require cross-engine catalog portability.
The transformation layer remains sufficiently isolated to support a future Iceberg
persistence adapter.

### A conventional SQL database

A relational database could support transactions and upserts, but it is not the
natural store for high-volume Spark transformations and evolving telemetry files.
It would also introduce a separate service outside the free Databricks environment.

SQL itself is not an alternative table format. It is a query language used against
systems that may store data in Delta, Iceberg, Parquet, or database-native formats.

## Consequences

- Multi-file table changes are committed atomically through the Delta transaction
  log.
- Corrected releases can update trusted business keys without rebuilding all history.
- Local integration tests can validate replay and correction behavior.
- The project depends on Delta/Spark compatibility and pins both to the same release
  line.
- Consumers outside the Delta ecosystem need a compatible reader or an exported
  representation.

## Reconsider when

Reconsider if the primary execution platform changes, multiple non-Spark engines
must write the tables, an organization mandates Iceberg, or catalog portability
becomes more important than native Databricks integration.
