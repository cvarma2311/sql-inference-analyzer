# sql-inference-analyzer

## How to Run

### 1) Start llama.cpp server

```bash
scripts/run_llama_server.sh
```

### 2) Run the NL Planner service

```bash
uvicorn services.nl_planner_service.main:app --reload --port 8000
```

### 3) Generate SQL from NL (NL2SQL)

```bash
uvicorn services.nl2sql_service.main:app --reload --port 8002
```

```bash
curl -s http://localhost:8002/nl2sql \
  -H 'Content-Type: application/json' \
  -d '{"question":"How is Retail doing this FY vs target and what are the drivers?","fiscal_year":"2025-2026","start_date":"2025-04-01","end_date":"2025-10-06"}'
```

### 4) Send a planner-only request

```bash
curl -s http://localhost:8000/plan \
  -H 'Content-Type: application/json' \
  -d '{"question":"How is Retail doing this FY vs target and what are the drivers?"}'
```

### 5) Generate a narrative

```bash
uvicorn services.narrative_service.main:app --reload --port 8003
```

```bash
curl -s http://localhost:8003/narrative \
  -H 'Content-Type: application/json' \
  -d '{"kpi_results":{"achievement_pct":84.0,"gap_tmt":-0.21},"flags":["UNDERPERFORMANCE","DECLINING_TREND"]}'
```

### 6) Run end-to-end pipeline

```bash
uvicorn services.pipeline_service.main:app --reload --port 8004
```

```bash
curl -s http://localhost:8004/analyze \
  -H 'Content-Type: application/json' \
  -d '{"question":"How is Retail doing this FY vs target and what are the drivers?","fiscal_year":"2025-2026","start_date":"2025-04-01","end_date":"2025-10-06"}'
```

### Optional: stop llama.cpp server

```bash
scripts/run_llama_server.sh --stop
```

## .env Required Fields

- `DB_HOST`
- `DB_PORT`
- `DB_NAME`
- `DB_USER`
- `DB_PASSWORD`
- `DB_SSLMODE` (optional, leave empty if not required)
- `DB_SCHEMA` (optional, default: `public`)

Use `.env.example` as a template for `.env`.

## Optional Environment Overrides

- `LLM_URL` (default: `http://localhost:8001/v1/chat/completions`)
- `LLM_MODEL` (default: `local`)
- `LLM_TIMEOUT_S` (default: `30`)
- `LLM_RETRIES` (default: `2`)
- `LLM_RETRY_BACKOFF_S` (default: `0.5`)
- `LOG_FORMAT` (`json` or `text`, default: `json`)
- `LOG_LEVEL` (default: `INFO`)
- `LOG_FILE` (optional file path for logs)
- `PLANNER_SCHEMA_CONTEXT` (`on` or `off`, default: `on`)
- `LOG_PLAN_OUTPUT` (`on` or `off`, default: `on`)
- `SQL_MAX_ROWS` (default: `1000`)
- `LOG_SQL_OUTPUT` (`on` or `off`, default: `on`)
- `NARRATIVE_MAX_TOKENS` (default: `256`)
- `LOG_NARRATIVE_OUTPUT` (`on` or `off`, default: `on`)
- `DEFAULT_START_DATE` (required for pipeline)
- `DEFAULT_END_DATE` (required for pipeline)
- `DEFAULT_FISCAL_YEAR` (optional, format: `YYYY-YYYY`, example: `2025-2026`)
- `AUDIT_LOG_DIR` (default: `audit_logs`)

## Logging Correlation

- Pass a `request_id` into the SQL executor to correlate `sql_execute_*` logs with planner logs.

## Project Structure

```
.
├── Implementation-plans/
│   ├── offline-nl2sql-inference-implementation.md
│   └── steps/
├── artifacts/
│   └── Sales Schema and Parameters.pdf
├── config/
├── external/
│   └── llama.cpp/
├── models/
│   └── llm/
├── scripts/
│   ├── db_check.py
│   └── run_llama_server.sh
├── services/
│   ├── nl_planner_service/
│   │   ├── README.md
│   │   ├── llm_client.py
│   │   ├── logging.py
│   │   ├── main.py
│   │   ├── plan_schema.json
│   │   ├── prompt.py
│   │   ├── schemas.py
│   │   └── settings.py
│   ├── nl2sql_service/
│   │   ├── README.md
│   │   └── main.py
│   ├── narrative_service/
│   │   ├── README.md
│   │   ├── llm_client.py
│   │   ├── prompt.py
│   │   ├── service.py
│   │   └── settings.py
│   ├── pipeline_service/
│   │   ├── README.md
│   │   └── main.py
│   ├── audit_log_service/
│   │   ├── README.md
│   │   └── audit_log.py
│   └── sql_executor_service/
│       ├── README.md
│       └── executor.py
├── .env.example
├── .gitignore
├── README.md
└── requirements.txt
```
