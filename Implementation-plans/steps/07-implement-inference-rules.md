# Step 07 - Implement Inference Rules

## Goal
Compute deterministic KPIs and flags without using the LLM.

## Inputs
- SQL results from the executor.
- Rule thresholds (e.g., achievement %, decline rules).

## Actions
- Implement Python rule engine for KPI comparisons.
- Produce structured output with flags.

## Exit Criteria
- Given test data, rules produce expected flags and metrics.
