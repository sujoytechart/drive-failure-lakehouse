# ADR 0005: Use merge-based Silver idempotency

- Status: Accepted

## Context

Backblaze may republish corrected records, and an operator may rerun a workflow after
a partial or uncertain failure. The trusted Silver grain is one row per
`(date, serial_number)`. Reprocessing must not duplicate that business key, while a
later approved source revision must be able to replace an earlier value.

## Decision

Silver is written with a Delta `MERGE` keyed by `date` and `serial_number`.
Quarantine uses the same operation keyed by deterministic `quarantine_id`.

Before a merge begins, the source DataFrame must contain every merge key and must be
unique on the complete key. Matching records update all target columns. New records
are inserted. Source selection happens before persistence, using explicit revision,
file timestamp, and filename precedence.

Gold does not use merge. It is fully reproducible from Silver and is atomically
replaced so rows that disappear from a recomputation cannot remain stale.

## Alternatives considered

### Append every accepted record

Append is appropriate for the Bronze audit history but would create duplicate Silver
business keys on replay and force every consumer to resolve corrections.

### Overwrite the complete Silver table

Full overwrite is simple for a tiny demonstration but scales with all historical
data and risks removing unaffected history when an input scope is incomplete.

### Replace affected partitions

Partition replacement can be efficient when corrections align perfectly with a
stable partition boundary. A corrected release can overlap arbitrary drive-days, and
incorrect replacement predicates can remove valid records. Keyed merge states the
business contract more directly.

### Let Delta resolve duplicate source keys

A merge with multiple source rows matching one target row is ambiguous and can fail
or behave differently as implementations evolve. Duplicate resolution is a domain
decision, so it occurs before the storage operation. Unresolved top-precedence
conflicts are quarantined rather than delegated to the engine.

## Consequences

- Replaying identical accepted or rejected input does not increase target row counts.
- Later corrections update only matching business keys.
- Merge validation performs an additional distributed duplicate check.
- Schema changes must be deliberate. `updateAll` and `insertAll` expect a compatible
  target contract.
- Concurrent writers still rely on Delta optimistic concurrency and may require a
  workflow retry.

## Reconsider when

Reconsider if data volume makes per-key merge uneconomical, corrections become
strictly partition-scoped, change-data-capture semantics are required, or multiple
concurrent writers need a different serialization strategy.
