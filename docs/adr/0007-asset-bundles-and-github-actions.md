# ADR 0007: Deploy with Asset Bundles and GitHub Actions

- **Status:** Accepted
- **Date:** 2026-09-03

## Context

The Databricks jobs, dependencies, schedule, parameters, and wheel artifact must be
reviewed and reproducible rather than assembled manually in the workspace. Deployment
credentials must remain outside source control, and a learning workspace should not be
changed automatically by every commit.

## Decision

Define Databricks resources in an Asset Bundle and deploy the development target with a
manually dispatched GitHub Actions workflow. The workflow installs Python 3.12, `uv`,
and the official Databricks CLI action; validates the bundle; deploys it; and prints a
resource summary.

Store `DATABRICKS_HOST` and `DATABRICKS_TOKEN` as secrets in a GitHub environment named
`databricks-dev`. The workflow receives read-only repository permissions and serializes
deployments with a concurrency group. Deployment does not run the data pipeline, and
the declared pipeline schedule remains paused by default.

The pull-request CI workflow separately runs linting, formatting, strict type checks,
local Spark and Delta tests, and coverage without cloud credentials.

## Alternatives considered

- **Configure jobs in the Databricks UI:** useful for exploration, but creates resource
  drift and leaves reviewers without a versioned definition.
- **Deploy automatically on every merge:** common for a persistent development
  environment, but it can consume limited workspace quota and change cloud state without
  an explicit decision.
- **Expose workspace secrets to pull-request validation:** provides earlier cloud
  feedback, but unnecessarily expands credential exposure; local CI already validates
  the code and transformation behavior.
- **Use Terraform:** valuable for broader account and cloud infrastructure, but adds a
  second resource model where an Asset Bundle already owns these Databricks jobs.
- **Use service-principal OAuth:** preferred for a shared production deployment, but a
  personal token is the smaller setup for a single-user development workspace.

## Consequences

- A deployment is explicit, auditable, and repeatable from GitHub Actions.
- Repository contributors cannot access the workspace token from source or ordinary CI.
- Bundle validation against the workspace happens only during an authorized deployment.
- Personal-token rotation requires updating the GitHub environment secret.
- Production adoption would require environment separation, least-privilege service
  principals, approvals, and an automatic promotion policy.

## Reconsider when

The workspace becomes shared, deployments become frequent, or the project gains staging
and production targets with formal promotion requirements.
