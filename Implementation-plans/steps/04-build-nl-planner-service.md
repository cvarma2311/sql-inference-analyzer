# Step 04 - Build NL Planner Service

## Goal
Translate natural language into a structured plan JSON.

## Inputs
- NL prompt schema and JSON output contract.
- Model endpoint (llama.cpp server).

## Actions
- Create the service skeleton:
  - `mkdir -p services/nl_planner_service`
- Add strict JSON schema validation:
  - `services/nl_planner_service/plan_schema.json`
- Add prompts:
  - `services/nl_planner_service/prompt.py`
- Add LLM client with retries/backoff:
  - `services/nl_planner_service/llm_client.py`
- Add settings loader:
  - `services/nl_planner_service/settings.py`
- Add structured logging:
  - `services/nl_planner_service/logging.py`
- Add schema context injection:
  - `services/nl_planner_service/schema_context.py`
- Add FastAPI app + local function:
  - `services/nl_planner_service/main.py`
- Add usage docs:
  - `services/nl_planner_service/README.md`

## How To Run

- Local function:
  - `python -c "from services.nl_planner_service.main import plan_question; print(plan_question('How is Retail doing this FY vs target and what are the drivers?'))"`
- FastAPI:
  - `uvicorn services.nl_planner_service.main:app --reload --port 8000`
  - `curl -s http://localhost:8000/plan -H 'Content-Type: application/json' -d '{"question":"How is Retail doing this FY vs target and what are the drivers?"}'`
- Structured logging:
  - `LOG_FORMAT=json LOG_LEVEL=INFO uvicorn services.nl_planner_service.main:app --port 8000`
- File logging:
  - `LOG_FILE=/tmp/nl-planner.log LOG_FORMAT=json uvicorn services.nl_planner_service.main:app --port 8000`
- Request IDs:
  - `curl -s http://localhost:8000/plan -H 'Content-Type: application/json' -H 'X-Request-ID: demo-123' -d '{"question":"How is Retail doing this FY vs target and what are the drivers?"}'`

## Exit Criteria
- A sample NL question returns a valid plan JSON with required fields.
