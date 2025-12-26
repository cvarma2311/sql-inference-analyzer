# Narrative Service (LLM)

## Goal
Generate a grounded narrative from deterministic KPI results and flags.

## Local Usage

```bash
python -c "from services.narrative_service.service import generate_narrative; print(generate_narrative({'achievement_pct':84.0,'gap_tmt':-0.21}, ['UNDERPERFORMANCE','DECLINING_TREND']))"
```

```bash
python -c "from services.narrative_service.service import generate_bullet_summary; print(generate_bullet_summary({'achievement_pct':84.0,'gap_tmt':-0.21}, ['UNDERPERFORMANCE'], {'level':'sbu'}, {'underperforming_top':[],'overperforming_top':[]}))"
```

## API

```bash
uvicorn services.narrative_service.main:app --reload --port 8003
```

```bash
curl -s http://localhost:8003/narrative \
  -H 'Content-Type: application/json' \
  -d '{"kpi_results":{"achievement_pct":84.0,"gap_tmt":-0.21},"flags":["UNDERPERFORMANCE","DECLINING_TREND"]}'
```

## Environment

- `LLM_URL` (default: `http://localhost:8001/v1/chat/completions`)
- `LLM_MODEL` (default: `local`)
- `LLM_TIMEOUT_S` (default: `30`)
- `LLM_RETRIES` (default: `2`)
- `LLM_RETRY_BACKOFF_S` (default: `0.5`)
- `NARRATIVE_MAX_TOKENS` (default: `256`)
- `LOG_NARRATIVE_OUTPUT` (`on` or `off`, default: `on`)

## Notes

- The narrative is constrained to the provided KPI results and flags.
- No SQL or DB context is included in the prompt.
- Bullet summaries accept a compact drivers payload (top under/over performers).
