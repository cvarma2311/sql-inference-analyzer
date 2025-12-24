# Step 05 - Create KPI and Schema Registries

## Goal
Define the allowed tables, columns, and KPI definitions for SQL generation.

## Inputs
- Database schema and KPI requirements.

## Actions
- Create `config/kpi_registry.yaml` with KPI definitions.
- Create `config/schema_registry.yaml` with allowed tables and columns.
- Verify files exist:
  - `ls -l config/kpi_registry.yaml config/schema_registry.yaml`

## Exit Criteria
- Registries load successfully at runtime.
- Registry contents match the actual database schema.
