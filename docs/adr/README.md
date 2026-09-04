# Architecture decision records

Architecture decision records explain the choices that shape this lakehouse and the
tradeoffs accepted with each choice.

| Number | Decision | Status |
| --- | --- | --- |
| [0001](0001-databricks-free-edition.md) | Use Databricks Free Edition and managed storage | Accepted |
| [0002](0002-delta-table-format.md) | Use Delta Lake as the table format | Accepted |
| [0003](0003-scheduled-medallion-batch.md) | Use a scheduled medallion batch pipeline | Accepted |
| [0004](0004-auto-loader-ingestion.md) | Use Auto Loader for incremental file ingestion | Accepted |
| [0005](0005-merge-based-idempotency.md) | Use merge-based Silver idempotency | Accepted |
| [0006](0006-packaged-pyspark-transforms.md) | Package PySpark transformations as Python modules | Accepted |
| [0007](0007-asset-bundles-and-github-actions.md) | Deploy with Asset Bundles and GitHub Actions | Accepted |
| [0008](0008-exposure-adjusted-failure-rate.md) | Publish an exposure-adjusted failure rate | Accepted |

Accepted records are immutable except for spelling and link corrections. A decision
that changes an accepted record is documented in a new ADR that supersedes it.
