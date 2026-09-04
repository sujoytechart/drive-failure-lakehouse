# ADR 0003: Use a scheduled medallion batch pipeline

- **Status:** Accepted
- **Date:** 2026-09-03

## Context

Backblaze publishes drive observations as daily CSV files. The selected releases are
bounded inputs rather than an always-on event stream. The pipeline still needs clear
recovery boundaries: raw ingestion must finish before trusted records are resolved,
and trusted records must finish before reporting metrics are replaced.

## Decision

Use one Databricks Workflow with dependent Bronze, Silver, and Gold wheel tasks. Bronze
uses a bounded incremental run, Silver performs deterministic upserts, and Gold rebuilds
its fully derived reporting tables. A weekly schedule is declared but paused by default;
manual runs remain available without consuming workspace quota unexpectedly.

## Alternatives considered

- **Continuous Structured Streaming:** useful for low-latency events, but it would keep
  compute active for a file source whose freshness requirement is measured in days.
- **One monolithic task:** simpler to launch, but it removes task-level retry boundaries
  and makes failures harder to locate and recover independently.
- **An external orchestrator:** appropriate when coordinating multiple platforms, but
  unnecessary while every dependency is inside one Databricks workflow.
- **A large number of narrowly split jobs:** increases visual complexity and operational
  overhead without improving the three meaningful data contracts.

## Consequences

- A failed layer prevents downstream publication.
- Each layer has its own logs, retry policy, and observable boundary.
- The declared schedule must be enabled deliberately after its cadence and quota impact
  are reviewed.
- This design favors predictable completion and recovery over sub-day freshness.

## Reconsider when

Source delivery becomes continuous, a consumer requires materially lower latency, or
the workflow must coordinate systems outside Databricks.
