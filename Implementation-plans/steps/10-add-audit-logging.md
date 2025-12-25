# Step 10 - Add Audit Logging

## Goal
Store all inputs, outputs, and derived artifacts for traceability.

## Inputs
- User question, plan JSON, SQL, results, flags, narrative.

## Actions
- Add JSON audit logger:
  - `services/audit_log_service/audit_log.py`
- Add usage docs:
  - `services/audit_log_service/README.md`
- Wire into pipeline:
  - `services/pipeline_service/main.py`

## Exit Criteria
- Each request produces a complete, queryable audit record.
