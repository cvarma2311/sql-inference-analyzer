# Step 08 - Add Narrative LLM Call

## Goal
Generate a grounded narrative from deterministic outputs.

## Inputs
- KPI results and flags.
- LLM endpoint (offline).

## Actions
- Construct prompt that forbids recomputation and invention.
- Send only derived metrics and context to the LLM.

## Exit Criteria
- Narrative matches the computed results and avoids unsupported claims.
