# Step 09 - Add Audit Logging

## Goal
Store all inputs, outputs, and derived artifacts for traceability.

## Inputs
- User question, plan JSON, SQL, results, flags, narrative.

## Actions
- Define an audit schema (Postgres table or JSON files).
- Persist each request with hashes and timestamps.

## Exit Criteria
- Each request produces a complete, queryable audit record.
