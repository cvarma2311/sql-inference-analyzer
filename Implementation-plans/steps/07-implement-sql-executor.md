# Step 07 - Implement SQL Executor

## Goal
Execute generated SELECT queries safely and capture execution metadata.

## Inputs
- Validated SQL queries from the builder.
- Database connection settings (DATABASE_URL or DB_* env vars).

## Actions
- Create the executor service:
  - `services/sql_executor_service/executor.py`
- Add usage docs:
  - `services/sql_executor_service/README.md`
- Verify locally:
  - `python -c "from services.sql_executor_service.executor import execute_queries; print(execute_queries(['SELECT 1']))"`

## Exit Criteria
- Queries execute sequentially and return rows.
- Execution metadata includes row count, execution time, and a result hash.
