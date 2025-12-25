# SQL Executor Service

## Goal
Execute validated SELECT queries and capture execution metadata.

## Local Usage

```bash
python -c "from services.sql_executor_service.executor import execute_queries; print(execute_queries(['SELECT 1'], request_id='demo-123'))"
```

## Environment

- `DATABASE_URL` (preferred)
- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
- `DB_SSLMODE` (optional)
- `SQL_MAX_ROWS` (default: `1000`)
- `LOG_SQL_OUTPUT` (`on` or `off`, default: `on`)

## Outputs

Each result includes:

- `row_count`
- `execution_ms`
- `result_hash`
- `rows`

## Logging

- Pass `request_id` to `execute_queries` to correlate SQL logs across services.
