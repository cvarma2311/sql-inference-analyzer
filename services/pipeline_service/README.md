# Pipeline Service (End-to-End)

## Goal
Run the full chain: plan -> SQL -> execute -> inference -> narrative.

## API

```bash
uvicorn services.pipeline_service.main:app --reload --port 8004
```

```bash
curl -s http://localhost:8004/analyze \
  -H 'Content-Type: application/json' \
  -d '{"question":"How is Retail doing this FY vs target and what are the drivers?","fiscal_year":"2025-2026","start_date":"2025-04-01","end_date":"2025-10-06"}'
```

## Environment

- `DEFAULT_START_DATE` and `DEFAULT_END_DATE` (required if not provided in request)
- `DEFAULT_FISCAL_YEAR` (optional, format: `YYYY-YYYY`)
- All DB env vars for execution

## Output

- `plan`
- `sql`
- `execution`
- `inference`
- `narrative` (structured JSON with drivers drilldown)
- `summary_bullets` (LLM bullet summary)
