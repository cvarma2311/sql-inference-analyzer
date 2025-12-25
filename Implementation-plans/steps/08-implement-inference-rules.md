# Step 08 - Implement Inference Rules

## Goal
Compute deterministic KPIs and flags without using the LLM.

## Inputs
- SQL results from the executor.
- Rule thresholds (e.g., achievement %, decline rules).

## Actions
- Implement Python rule engine for KPI comparisons:
  - `services/inference_service/rules.py`
- Add usage docs:
  - `services/inference_service/README.md`
- Verify locally:
  - `python -c "from services.inference_service.rules import run_inference; print(run_inference(84.0, 100.0, [100, 95, 90], 35.0))"`

## Exit Criteria
- Given test data, rules produce expected flags and metrics.
