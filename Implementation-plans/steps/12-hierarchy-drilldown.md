# Step 12: Implement Hierarchy Drilldown (SBU -> Zone -> Region -> Sales Area -> Product)

## Objective

Add default two-level drilldown behavior and structured inference outputs based on the hierarchy and conversational specificity rules.

## Prerequisites

- Existing planner, SQL builder/validator, and inference services are in place.
- `config/schema_registry.yaml` includes `SBU_Name`, `Zone_Name`, `Region_Name`, `SalesArea_Name`, `ProductName`, and `fiscal_year`.

## Tasks

1. **Planner schema + rules**
   - Extend planner output with `drilldown_path`, `level`, `filters`, and `driver_levels`.
   - Implement default driver mapping:
     - `sbu` -> `["zone", "region"]`
     - `zone` -> `["region", "sales_area"]`
     - `region` -> `["sales_area", "product"]`
     - `sales_area` -> `["sales_area", "product"]`
     - `product` -> `[]`
   - If user mentions a specific entity (product/sales area/region/zone/sbu), set `level` and `filters` accordingly and apply default mapping.

2. **SQL templates**
   - Add rollup templates for actuals/targets by `level` using `fiscal_year`.
   - Add driver template `gap_drivers_by_driver_levels.sql.j2` that groups by `driver_levels`.
   - Ensure SQL uses only these hierarchy columns: `SBU_Name`, `Zone_Name`, `Region_Name`, `SalesArea_Name`, `ProductName`.

3. **SQL validator**
   - Allow dynamic `GROUP BY` columns sourced from the hierarchy registry.
   - Enforce mandatory constraints and `fiscal_year` filter.

4. **Inference outputs**
   - Return structured JSON (no paragraphs).
   - Include `top_negative_drivers` and `top_positive_drivers` with top 5 each.

5. **Tests**
   - Add tests for default mapping and entity-specific queries.
   - Validate SQL rendering and structured inference output shape.

## Acceptance Criteria

- User asking for SBU drivers results in drilldown to Zone + Region.
- User asking for Zone drivers results in drilldown to Region + Sales Area.
- User asking for Sales Area drivers results in Sales Area + Product drivers.
- User mentions a specific entity and the system focuses on that level with correct filters.
- Output is structured JSON with top 5 positive and negative contributors.
