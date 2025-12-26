# NL Planner Service

## Goal
Convert natural language questions into a strict JSON analysis plan.

## Local (Python)

```bash
python -c "from services.nl_planner_service.main import plan_question; print(plan_question('How is Retail doing this FY vs target and what are the drivers?'))"
```

## API

```bash
uvicorn services.nl_planner_service.main:app --reload --port 8000
```

```bash
curl -s http://localhost:8000/plan -H 'Content-Type: application/json' -d '{"question":"How is Retail doing this FY vs target and what are the drivers?"}'
```

## Output Fields

- `level`: one of `sbu`, `zone`, `region`, `sales_area`, `product`
- `filters`: optional hierarchy filters (e.g., `sbu: Retail`)
- `driver_levels`: ordered drilldown levels to `product`
- `fiscal_year`: normalized to `YYYY-YYYY` when provided

## Environment

- `LLM_URL` (default: `http://localhost:8001/v1/chat/completions`)
- `LLM_MODEL` (default: `local`)
- `LLM_TIMEOUT_S` (default: `30`)
- `LLM_RETRIES` (default: `2`)
- `LLM_RETRY_BACKOFF_S` (default: `0.5`)
- `LOG_FORMAT` (`json` or `text`, default: `json`)
- `LOG_LEVEL` (default: `INFO`)
- `LOG_FILE` (optional file path; when set logs are written to this file)
- `PLANNER_SCHEMA_CONTEXT` (`on` or `off`, default: `on`)
- `LOG_PLAN_OUTPUT` (`on` or `off`, default: `on`)

## Request IDs

- The service accepts `X-Request-ID` and returns it in the response.
- If missing, a UUID is generated per request.

## Schema Context

- The planner injects KPI and column descriptions from:
  - `config/kpi_registry.yaml`
  - `config/schema_registry.yaml`
- Disable with `PLANNER_SCHEMA_CONTEXT=off`.
