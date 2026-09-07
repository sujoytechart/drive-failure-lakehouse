# ADR 0001: Use Databricks Free Edition and managed storage

- Status: Accepted
- Date: 2026-09-03

## Context

This project needs a reproducible Databricks environment for technical evaluation
without requiring ongoing cloud infrastructure charges. The workload
is a bounded sample of public drive telemetry rather than a production service with
availability or support commitments.

[Databricks Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)
provides serverless compute and a Unity Catalog-enabled workspace at no cost, subject
to fair-use quotas and feature limitations.

## Decision

Use Databricks Free Edition as the deployment target. Jobs use serverless compute,
tables use managed Delta storage, and source samples, Auto Loader schema state, and
checkpoints use a managed Unity Catalog Volume.

The workspace host and local authentication profile are deployment inputs and are
never committed. Bundle variables identify the catalog and Bronze, Silver, and Gold
schemas without embedding account-specific values in source control.

Local PySpark and Delta tests remain the primary fast feedback loop. A small packaged
wheel job validates the actual Databricks boundary before the full workflow is run.

Pin serverless jobs to environment version 4. It provides Python 3.12 and the Spark 4
generation used by the local test environment, while retaining a published support
window. Declare the project wheel once in each job's serverless environment rather than
as task-level libraries, which serverless wheel tasks do not support.

## Alternatives considered

### Databricks free trial

A trial can expose more platform features for a limited period, but it introduces an
expiration deadline and can require billing-related setup. The selected pipeline does
not require that additional environment.

### Paid Databricks workspace

A paid workspace would support broader compute, networking, identity, and operational
controls. Those capabilities are not required to demonstrate this pipeline and would
create avoidable cost and account-management work.

### Local Spark only

Local Spark is sufficient for transformation development but cannot demonstrate
Declarative Automation Bundles, serverless jobs, Unity Catalog, managed Volumes, or
Databricks deployment automation.

## Consequences

- Free Edition provides only serverless compute and limits concurrent job tasks and
  daily usage. Quota exhaustion can pause compute until the quota resets.
- Free Edition provides no guaranteed reliability, support, or service-level
  agreement; the project makes no such claims.
- Serverless compute uses Spark Connect APIs, defaults to ANSI SQL behavior, limits
  DBFS access, and does not expose the classic Spark UI. These constraints favor
  DataFrame APIs, safe casts, Unity Catalog storage, and application-level metrics.
- The serverless environment version is an explicit compatibility boundary. Upgrading
  it requires rerunning the smoke job and the complete workflow before deployment.
- Managed storage avoids cloud credentials and external-location configuration.
- The bundle owns the project schemas and Volume. Destruction protection prevents an
  ordinary bundle teardown from deleting their data; intentional cleanup is manual.
- The bundle and core code remain portable to a fuller Databricks workspace, but
  production identity, network, observability, and recovery controls would require
  additional design.

## Reconsider when

Reconsider when the sample exceeds Free Edition quotas, classic or dedicated compute
is required, private networking or workload identity becomes mandatory, multiple
environments must be isolated, or the workload acquires production availability and
support requirements.
