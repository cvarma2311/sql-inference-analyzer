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

### 3) Send a request

```bash
curl -s http://localhost:8000/plan \
  -H 'Content-Type: application/json' \
  -d '{"question":"How is Retail doing this FY vs target and what are the drivers?"}'
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
│   └── nl_planner_service/
│       ├── README.md
│       ├── llm_client.py
│       ├── logging.py
│       ├── main.py
│       ├── plan_schema.json
│       ├── prompt.py
│       ├── schemas.py
│       └── settings.py
├── .env.example
├── .gitignore
├── README.md
└── requirements.txt
```
