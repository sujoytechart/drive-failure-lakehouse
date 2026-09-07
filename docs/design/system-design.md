# Drive Failure Lakehouse System Design

## Design approach

I kept the data path deliberately small so the project remains understandable and can
run in a free workspace. The engineering work is concentrated at the boundaries where
schema drift, corrected releases, invalid records, replay safety, and metric definitions
need explicit handling.

## Problem

This project turns daily hard-drive health records into reliable tables that
show how frequently each drive model fails. The pipeline runs on Databricks and
remains reproducible within a quota-limited, no-cost workspace.

Backblaze publishes one record per observed drive per day. A record identifies
the drive and model, reports whether it failed that day, and includes a changing
set of SMART health attributes. The source presents practical lakehouse
engineering concerns: high row volume, real schema drift, repeated
observations, and corrected data releases.

## Goals

- Build an end-to-end Bronze, Silver, and Gold pipeline on Databricks.
- Handle schema evolution without making the trusted schema unstable.
- Make reruns and corrected-source processing idempotent.
- Preserve invalid input with actionable rejection reasons.
- Publish understandable drive-model reliability measurements.
- Keep transformation logic testable outside Databricks.
- Deploy the workflow as code and validate changes with GitHub Actions.
- Explain consequential decisions through concise architecture decision records.

## Non-goals

- Predicting whether an individual drive will fail.
- Estimating lifetime survival probability with Kaplan-Meier or another
  survival-analysis model.
- Full-history ingestion. The default dataset is bounded to selected quarters.
- Adding orchestration or infrastructure layers that the batch workload does not need.
- Enterprise service-level objectives, compliance controls, private networking,
  and production-scale capacity planning.

## Constraints

- The deployment target is Databricks Free Edition with serverless compute.
- Input consists of small, reproducible samples from two widely separated
  Backblaze quarters that exhibit genuine SMART-column drift.
- Source data lands in a Unity Catalog Volume because Free Edition does not
  support custom workspace storage locations.

## Architecture

The system uses a direct data path:

```text
Backblaze CSV files
        |
        v
Unity Catalog landing Volume
        |
        v
Bronze raw Delta table
        |
        +----> Silver quarantine Delta table
        |
        v
Silver trusted drive-day Delta table
        |                  |
        v                  v
Gold daily model     Gold reporting-period
observations         metrics
```

A Databricks Workflow runs the Bronze, Silver, and Gold tasks in dependency
order. A Databricks Asset Bundle defines the jobs and their resources in source
control. GitHub Actions runs repository quality checks and deploys the bundle.

The design stays focused on the parts that affect data correctness. Those parts include
schema handling, deterministic conflict resolution, atomic writes, validation, and run
evidence.

## Source and landing layout

Each input row represents one physical drive on one observation date. Core
source fields include the date, serial number, model, capacity, and a failure
flag. SMART attributes arrive as pairs such as `smart_5_raw` and
`smart_5_normalized`. The set of attribute columns changes over time.

Landing paths identify both the source quarter and revision:

```text
/Volumes/<catalog>/<schema>/<volume>/landing/release=2024-Q1-r1/
/Volumes/<catalog>/<schema>/<volume>/landing/release=2024-Q1-r2/
```

Corrected files receive a new revision path. The original arrival is never
silently overwritten. Revision numbers are positive integers and higher
revisions take precedence for the same business key.

## Bronze layer

`bronze.drive_telemetry_raw` has one row per ingested source row and does not
enforce a business key. It preserves source columns together with:

- `source_file`
- `source_quarter`
- `source_revision`
- `file_modification_time`
- `ingested_at`
- rescued or corrupt input captured during ingestion

Auto Loader incrementally discovers landing files. Its schema location and
checkpoint are stored separately from the source files and tables. A bounded
`availableNow` run processes all currently available input and then stops,
matching the source's batch arrival pattern while retaining incremental file
tracking.

Bronze is an append-only audit history. A normal rerun with no new files adds no
rows because the Auto Loader checkpoint remembers previously processed file
identities.

## Silver layer

### Trusted table

`silver.drive_daily` has one row per `(date, serial_number)` and contains:

- typed core drive fields
- source and lineage metadata
- `quality_warnings`, an array of non-fatal warning codes
- `smart_attributes`, a map keyed by SMART attribute number whose value is a
  structure containing nullable `raw` and `normalized` numeric readings.

The map keeps the Silver schema stable when manufacturers introduce new SMART
attributes. Bronze remains the authoritative representation of the original
wide columns. A future feature-engineering consumer can materialize selected
attributes into a wide feature table without changing this pipeline's trusted
record contract.

### Deterministic conflict resolution

Before writing, Silver selects exactly one source row for each business key.
Candidates are ordered by:

1. source revision, with the latest applicable revision first
2. file modification time, newest first
3. source file path as a stable final tie-breaker.

If two rows remain indistinguishable under that ordering but disagree in their
business values, neither is selected silently. The conflict is quarantined.

The selected source is applied with a Delta `MERGE` keyed by
`(date, serial_number)`. New keys are inserted and later source revisions update
existing keys. Replaying the same accepted input therefore leaves Silver
unchanged.

### Quarantine table

`silver.drive_daily_quarantine` preserves rejected records with:

- source identity and release metadata
- ingestion time
- the original row encoded without discarding source values
- `rejection_reasons`, a non-empty array of stable reason codes.

A deterministic quarantine identifier prevents the same rejected source row
from being duplicated by a replay.

Critical validation failures include an invalid date, empty serial number,
failure flag outside `0` and `1`, non-positive capacity, and an unresolved
business-key conflict. An unparseable optional SMART value does not invalidate
the entire drive-day record: Bronze preserves the original value, Silver omits
that map value, and `quality_warnings` records the issue.

## Gold layer

Gold tables are flat and wide because they serve SQL queries, dashboards, and
downstream analytics. Flexible SMART telemetry does not flow into these
consumption tables.

### Daily observations

`gold.drive_model_daily` has one row per `(date, model)` and publishes additive
measurements:

- `active_drives`
- `failures`
- `drive_days`

One valid daily observation contributes one drive-day. The failure day is
included because the drive was observed at risk during that reporting day.

### Reporting-period metrics

`gold.drive_model_period` has one row per model and explicit reporting window.
It publishes:

- `period_start`
- `period_end`
- `model`
- `unique_drives_observed`
- `failed_drives`
- `total_drive_days`
- `naive_failed_drive_percentage`
- `failures_per_million_drive_days`

The measures are defined as:

```text
naive_failed_drive_percentage
  = 100 * failed_drives / unique_drives_observed

failures_per_million_drive_days
  = 1,000,000 * failed_drives / total_drive_days
```

The first measure is intuitive but ignores how long drives were observed. The
second accounts for exposure time but is an incidence rate, not a lifetime
failure probability or a complete correction for right censoring. Survival
analysis is outside the system boundary because it requires separate cohort,
drive-age, entry, and exit semantics.

## Failure behavior and observability

The workflow fails rather than publishing questionable output when:

- required source columns are absent
- duplicate Silver keys remain after deterministic resolution
- Gold produces negative counts or a rate with a non-positive denominator
- a Delta transaction cannot commit.

Tasks report Auto Loader progress or relevant input and output row counts. Task
status, source lineage, reporting window, and stable reason codes make a run
understandable without inspecting individual records.

Delta transactions provide atomic table changes. A failed task does not expose
a partially written target table. Workflow dependencies prevent Gold from
running when Silver fails.

## Code boundaries

```text
src/drive_failure_lakehouse/
  smart.py              changing wide SMART columns to a stable map
  quality.py            validation, warnings, and quarantine reasons
  silver.py             typing, conflict resolution, and trusted records
  gold.py               daily and reporting-period aggregations
  jobs/
    bronze.py           Auto Loader and Bronze write orchestration
    silver.py           Silver merge and quarantine orchestration
    gold.py             Gold table write orchestration

tests/
  fixtures/             small representative source schemas
  unit/                 transformation behavior
  integration/          Delta write and replay behavior

resources/
  workflow.yml          Databricks Workflow resource definition
  storage.yml           Unity Catalog schemas and managed Volume
```

Transformation modules accept and return Spark DataFrames and do not reach into
workspace APIs. Job entry points own platform configuration and table writes. This
allows the transformation logic to be tested locally while platform-specific behavior
is verified through Databricks smoke and end-to-end runs.

Jobs are packaged as Python wheel tasks and run on serverless compute.

## Testing strategy

GitHub Actions runs Ruff, mypy, pytest, and applicable local Delta integration
tests. Tests cover behavior rather than implementation details:

- two representative source shapes produce the same Silver schema
- newly encountered SMART attributes become map entries
- critical invalid records receive explicit reasons
- optional malformed SMART readings produce warnings
- duplicate selection is deterministic
- replaying accepted input is idempotent
- a later release replaces the earlier business record
- daily and reporting-period metrics match hand-calculated examples
- empty inputs and zero-denominator reporting windows behave explicitly.

A Databricks smoke task validates the active catalog and schema boundary. An
end-to-end run and immediate rerun verify that Silver and Gold contents remain
stable when no new files arrive.

## Delivery

`.github/workflows/ci.yml` runs repository checks on pull requests and pushes.
`.github/workflows/deploy.yml` validates and deploys the Asset Bundle when
manually dispatched. Deployment creates or updates resources but does not
automatically run the data workflow.

The data workflow is manually triggered for controlled executions. GitHub
Secrets hold workspace authentication values and never enter source control.
A shared production deployment would use workload identity or a service principal
instead of a personal development credential.

## Architecture decisions

The decisions that shape the implementation are documented separately so their context,
alternatives, and consequences remain easy to review:

1. [Databricks Free Edition and managed storage](../adr/0001-databricks-free-edition.md)
2. [Delta Lake versus Iceberg and raw Parquet](../adr/0002-delta-table-format.md)
3. [Scheduled batch processing versus streaming or external orchestration](../adr/0003-scheduled-medallion-batch.md)
4. [Auto Loader versus `COPY INTO` or a custom manifest](../adr/0004-auto-loader-ingestion.md)
5. [Merge-based idempotency versus append or overwrite strategies](../adr/0005-merge-based-idempotency.md)
6. [Packaged transformations versus notebook-only code](../adr/0006-packaged-pyspark-transforms.md)
7. [Asset Bundles and GitHub Actions versus manual deployment](../adr/0007-asset-bundles-and-github-actions.md)
8. [Exposure-adjusted incidence versus survival analysis](../adr/0008-exposure-adjusted-failure-rate.md)
