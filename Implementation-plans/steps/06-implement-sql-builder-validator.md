# Step 06 - Implement SQL Builder and Validator

## Goal
Generate safe, deterministic SQL from templates and enforce constraints.

## Inputs
- Query templates (Jinja2).
- Registries and mandatory constraints.

## Actions
- Add mandatory constraints fragment:
  - `config/rules/mandatory_constraints.sql`
- Add Jinja2 templates:
  - `config/query_templates/actual_by_month.sql.j2`
  - `config/query_templates/target_by_month.sql.j2`
- Add registry loader:
  - `services/sql_builder_service/registry_loader.py`
- Add template renderer:
  - `services/sql_builder_service/template_renderer.py`
- Add SQL validator:
  - `services/sql_builder_service/validator.py`
- Add SQL builder:
  - `services/sql_builder_service/builder.py`
- Add usage docs:
  - `services/sql_builder_service/README.md`
- Verify locally:
  - `python -c "from services.sql_builder_service.builder import build_query; print(build_query('actual_by_month.sql.j2', {'sbu':'Retail','start_date':'2025-04-01','end_date':'2025-10-06'}))"`

## Exit Criteria
- Valid templates render correctly for known inputs.
- Invalid SQL is rejected with clear errors.
