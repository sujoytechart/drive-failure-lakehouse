# ADR 0006: Package PySpark transformations as Python modules

- Status: Accepted

## Context

The transformations need to run on Databricks while remaining understandable and
testable on a developer machine and in GitHub Actions. Business rules should not depend
on notebook state, display commands, or manually executed cells.

## Decision

Implement transformation logic as typed Python modules that accept and return Spark
DataFrames. Keep Databricks entry points thin: they parse validated configuration,
obtain the active Spark session, invoke the transformations, and perform Delta writes.
Build the package as a wheel and run each workflow layer as a Python wheel task.

PySpark and Delta Lake are development dependencies locally and platform-provided
dependencies on Databricks. This avoids bundling large runtime libraries into the
application wheel while keeping local versions explicitly constrained.

## Alternatives considered

- **Workspace notebooks:** effective for exploration, but hidden cell order and mutable
  state make modular tests, review, and reuse less reliable.
- **One procedural Spark script:** deployable, but it mixes domain rules with session,
  argument, logging, and storage concerns.
- **SQL-only transformations:** attractive for relational logic, but less suitable for
  normalizing a changing family of SMART columns into a typed map.
- **Additional framework abstractions:** could standardize larger estates, but would add
  ceremony without solving a current pipeline requirement.

## Consequences

- Core rules can be tested with local Spark without a Databricks account.
- The wheel is the same versioned artifact used by every workflow task.
- Job modules remain responsible only for orchestration and infrastructure boundaries.
- Runtime compatibility must be checked whenever local Spark or Delta versions change.

## Reconsider when

Most transformations become SQL-centric, the project joins an established framework,
or platform runtime libraries are no longer compatible with the package contract.
