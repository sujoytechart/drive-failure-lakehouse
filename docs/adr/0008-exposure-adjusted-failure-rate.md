# ADR 0008: Publish an exposure-adjusted failure rate

- Status: Accepted

## Context

Backblaze data contains one row for each day a drive is observed. Comparing only
the percentage of drives that failed can be misleading because models may have
different fleet sizes and observation durations. A model observed for one million
drive-days has had more opportunity to fail than a model observed for ten thousand
drive-days.

The Gold layer needs a measurement that remains understandable in SQL and dashboard
use while representing this difference in exposure.

## Decision

The reporting-period table publishes both:

- `naive_failed_drive_percentage`, calculated as distinct failed drives divided by
  distinct observed drives, multiplied by 100
- `failures_per_million_drive_days`, calculated as distinct failed drives divided by
  observed drive-day rows, multiplied by 1,000,000.

The daily table separately publishes active drives, failures, and drive-days at
`(date, model)` grain so consumers can recompute additive period summaries.

The exposure-adjusted value is described as an incidence rate. It is not described
as a failure probability, annualized failure rate, or censoring-correct estimate.

## Alternatives considered

### Failed-drive percentage only

This measure is easy to explain but ignores how long each model was observed. It is
retained as a familiar descriptive value, not as the primary comparison measure.

### Annualized failure rate

Annualization is common in drive reporting, but it adds a time-scaling assumption
that is unnecessary for this bounded sample. Publishing the observed rate per
million drive-days keeps the unit explicit and the arithmetic reproducible.

### Kaplan-Meier survival analysis

Survival analysis can account for right-censoring and estimate survival over drive
age. The available source does not reliably identify installation dates or complete
lifetime cohorts for every drive. Applying Kaplan-Meier without those semantics
would create false precision.

## Consequences

- Models with different observation exposure can be compared on a common unit.
- The numerator and denominator remain directly traceable to Silver records.
- A drive that fails contributes one failed drive and one observed drive-day on its
  failure date within the selected window.
- The metric does not control for drive age, workload, environment, model capacity,
  or incomplete observation before and after the selected period.
- Small denominators can produce unstable rates and should be displayed with the
  corresponding drive-day count.

## Reconsider when

Reconsider this decision when reliable installation dates, retirement dates, and
complete drive-age histories are available, or when the product requirement changes
from descriptive fleet incidence to lifetime reliability estimation. At that point,
cohort analysis or Kaplan-Meier survival curves should complement or replace this
summary rate.
