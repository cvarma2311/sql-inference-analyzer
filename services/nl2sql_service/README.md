# NL2SQL Service

## Goal
Convert a natural-language question into validated SQL statements.

## API

```bash
uvicorn services.nl2sql_service.main:app --reload --port 8002
```

```bash
curl -s http://localhost:8002/nl2sql \
  -H 'Content-Type: application/json' \
  -d '{"question":"How is Retail doing this FY vs target and what are the drivers?","fiscal_year":"2025-2026","start_date":"2025-04-01","end_date":"2025-10-06"}'
```

## Notes

- SQL is generated from templates and validated.
- `fiscal_year` is normalized to `YYYY-YYYY` if provided as `YYYY-YY`.
- The generated SQL is logged with `sql_generated`.
