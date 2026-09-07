# ADR 0004: Use Auto Loader for incremental file ingestion

- Status: Accepted

## Context

The landing area contains CSV files from releases with different SMART columns. Reruns
must discover new files without appending previously processed files again, while the
raw layer must retain new source columns and malformed-field evidence.

## Decision

Use Databricks Auto Loader in a bounded `availableNow` run. Store inferred schema state
and streaming checkpoints in separate paths inside the managed Volume. Permit additive
columns, keep inferred CSV values as strings in Bronze, and capture fields that do not
fit the inferred schema in a rescued-data column.

Every accepted landing path includes `release=YYYY-QN-rN`. Bronze derives the source
quarter and positive revision from that path and fails on a file outside this contract.
Corrected files are delivered under a higher revision rather than overwriting history.

## Alternatives considered

- **A plain batch read over the entire directory:** concise, but it requires custom file
  bookkeeping to make repeated runs incremental and replay-safe.
- **A hand-maintained file manifest:** offers precise control, but duplicates discovery,
  checkpoint, and failure-recovery behavior already provided by Auto Loader.
- **`COPY INTO`:** a reasonable SQL-oriented ingestion choice, but less natural for the
  PySpark package and schema-normalization path used throughout this project.
- **A fixed wide schema:** easy for one release, but brittle when SMART columns are added
  or omitted by later releases.

## Consequences

- Normal reruns process only files not recorded by the checkpoint.
- Schema and checkpoint locations become durable operational state and must not be
  deleted casually.
- New columns remain visible in Bronze without forcing the Silver contract to widen.
- Source corrections require a new immutable release path and revision.

## Reconsider when

The source moves away from file delivery, strict centrally managed schemas replace
source-led evolution, or ingestion must operate outside Databricks.
