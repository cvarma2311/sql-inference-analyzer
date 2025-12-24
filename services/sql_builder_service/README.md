# SQL Builder Service

## Goal
Render deterministic SQL from templates and validate against registries and constraints.

## Local Usage

```bash
python -c "from services.sql_builder_service.builder import build_query; print(build_query('actual_by_month.sql.j2', {'sbu':'Retail','start_date':'2025-04-01','end_date':'2025-10-06'}))"
```

## Templates

- `config/query_templates/actual_by_month.sql.j2`
- `config/query_templates/target_by_month.sql.j2`

## Validation Rules

- Query must start with SELECT.
- Mandatory constraints must be present.
- Only whitelisted tables and columns.
- Aggregations require GROUP BY.
- Date filters must use BETWEEN on date columns.

## Inputs

Template context must include:

- `sbu`
- `start_date`
- `end_date`

## Outputs

- Rendered SQL string.
- Validation result (boolean + errors).
