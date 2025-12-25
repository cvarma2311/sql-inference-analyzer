# Step 09 - Add Narrative LLM Call

## Goal
Generate a grounded narrative from deterministic outputs.

## Inputs
- KPI results and flags.
- LLM endpoint (offline).

## Actions
- Add prompt templates:
  - `services/narrative_service/prompt.py`
- Add LLM client:
  - `services/narrative_service/llm_client.py`
- Add narrative generator:
  - `services/narrative_service/service.py`
- Add settings:
  - `services/narrative_service/settings.py`
- Add usage docs:
  - `services/narrative_service/README.md`
- Verify locally:
  - `python -c "from services.narrative_service.service import generate_narrative; print(generate_narrative({'achievement_pct':84.0,'gap_tmt':-0.21}, ['UNDERPERFORMANCE','DECLINING_TREND']))"`

## Exit Criteria
- Narrative matches the computed results and avoids unsupported claims.
