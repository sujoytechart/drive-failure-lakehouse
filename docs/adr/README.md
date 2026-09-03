# Architecture decision records

Architecture decision records explain the choices that shape this lakehouse and the
tradeoffs accepted with each choice.

| Number | Decision | Status |
| --- | --- | --- |
| [0001](0001-databricks-free-edition.md) | Use Databricks Free Edition and managed storage | Proposed |
| [0002](0002-delta-table-format.md) | Use Delta Lake as the table format | Proposed |
| [0003](0003-scheduled-medallion-batch.md) | Use a scheduled medallion batch pipeline | Proposed |
| [0004](0004-auto-loader-ingestion.md) | Use Auto Loader for incremental file ingestion | Proposed |
| [0005](0005-merge-based-idempotency.md) | Use merge-based Silver idempotency | Proposed |
| [0006](0006-packaged-pyspark-transforms.md) | Package PySpark transformations as Python modules | Proposed |
| [0007](0007-asset-bundles-and-github-actions.md) | Deploy with Asset Bundles and GitHub Actions | Proposed |
| [0008](0008-exposure-adjusted-failure-rate.md) | Publish an exposure-adjusted failure rate | Proposed |

Accepted records are immutable except for spelling and link corrections. A decision
that changes an accepted record is documented in a new ADR that supersedes it.
