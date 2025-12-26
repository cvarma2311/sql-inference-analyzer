# SBU -> Zone -> Region -> Sales Area -> Product Drilldown

## Goal

Enable hierarchical drilldowns (SBU -> Zone -> Region -> Sales Area -> Product) as first-class drivers for inference, so underperformance can be attributed to specific products/areas at each level.

## Assumptions

- Source tables remain `MOM_DAY_LEVEL_DATA` (actuals) and `M60_LEVEL_METADATA` (targets).
- Existing deterministic SQL + inference rules stay in place.
- Time filtering still applies (e.g., FYTD, month range), but the primary drilldown axis is the org/product hierarchy.
- The hierarchy must use the following display columns only:
  - `SBU_Name`, `Zone_Name`, `Region_Name`, `SalesArea_Name`, `ProductName`
- Use `fiscal_year` for time slicing unless explicitly overridden by a future requirement.

## Data Contract Updates

1. **Hierarchy mapping**
   - Use only these columns in both tables:
     - `SBU_Name`, `Zone_Name`, `Region_Name`, `SalesArea_Name`, `ProductName`
   - If any table lacks a column, map it explicitly in the schema registry (e.g., `ORGSBUNAME -> SBU_Name`).

2. **Schema registry**
   - Add a `hierarchy` section to `config/schema_registry.yaml` for allowed dimension columns and display labels.
   - Ensure both tables have the required columns whitelisted.

## Planner Changes

1. **NL planner output schema**
   - Extend planner JSON with:
     - `drilldown_path`: ordered list of levels (always SBU -> Zone -> Region -> SalesArea -> Product).
     - `level`: requested focus level (e.g., `zone`, `sales_area`, `product`).
     - `filters`: optional selectors for upper levels (e.g., `sbu=Retail`, `zone=South`).
     - `driver_levels`: default two-level drilldown based on `level` unless user specifies otherwise.
   - Example:
     ```json
     {
       "objective": "actual_vs_target",
       "time_range": "current_fy_to_date",
       "drilldown_path": ["sbu", "zone", "region", "sales_area", "product"],
       "level": "sales_area",
       "driver_levels": ["product"],
        "filters": { "sbu": "Retail", "zone": "North" },
        "queries": ["actual_by_level", "target_by_level"]
     }
     ```

2. **Validation**
   - Enforce that requested `level` is one of the hierarchy levels.
   - Enforce that filters are only for higher levels than `level`.
   - Enforce `driver_levels` per default mapping unless overridden by explicit user request.

3. **Default two-level driver mapping**
   - If the user asks for drivers at a given level and does not override, use:
     - `sbu` -> `["zone", "region"]`
     - `zone` -> `["region", "sales_area"]`
     - `region` -> `["sales_area", "product"]`
     - `sales_area` -> `["sales_area", "product"]`
     - `product` -> `[]`
   - If the user explicitly names a different hierarchy level in the request, set `level` to that and apply the mapping above from that level.

4. **Conversational specificity**
   - If the user mentions a specific entity (e.g., product, sales area, region), set:
     - `level` to that entity's hierarchy level.
     - `filters` to include the mentioned entity.
     - `driver_levels` from the default mapping for that level.
   - Example: "Why is Product X underperforming in North?"
     - `level`: `product`
     - `filters`: `{ "product": "X", "zone": "North" }`
     - `driver_levels`: `[]`

## SQL Template Additions

1. **Parameterized rollups**
   - Add generic templates for actuals and targets that accept:
     - `group_by_columns` (based on requested level)
     - `filters` (upper-level selections)
     - `fiscal_year` (required)
   - Example template names:
     - `actual_by_level.sql.j2`
     - `target_by_level.sql.j2`

2. **Driver extraction (default two levels deeper)**
   - Add a template to compute contribution to gap at `driver_levels` derived from the default mapping.
     - Example: if level is `zone`, compute gap by `region` and `sales_area` within each zone.
   - Template name: `gap_drivers_by_driver_levels.sql.j2`

3. **SQL validator**
   - Update validator to allow dynamic `GROUP BY` columns from registry-defined hierarchy.

## Inference Logic Updates

1. **Hierarchy-aware driver rules**
   - Compute achievement and gap at the requested level.
   - Identify top negative and top positive contributors at `driver_levels`.
   - Rules:
     - `UNDERPERFORMANCE` if achievement < threshold (existing).
     - `DRIVER_LEVEL` set to the deepest level evaluated.
     - `TOP_NEGATIVE_DRIVERS` list of entities (e.g., products) with largest negative gap (top 5).
     - `TOP_POSITIVE_DRIVERS` list of entities (e.g., products) with largest positive gap (top 5).

2. **Structured inference output**
   - Replace paragraph narrative with a structured JSON payload.
   - Example schema:
     ```json
     {
       "level": "zone",
       "filters": { "sbu": "Retail" },
       "fiscal_year": "FY2024",
       "achievement_pct": 84.0,
       "gap_tmt": -0.21,
       "driver_level": "product",
       "top_negative_drivers": [
         { "product": "X", "sales_area": "Y", "gap_tmt": -0.08, "contribution_pct": 38.1 }
       ],
       "top_positive_drivers": [
         { "product": "Z", "sales_area": "W", "gap_tmt": 0.05, "contribution_pct": 23.8 }
       ],
       "flags": ["UNDERPERFORMANCE"]
     }
     ```
   - Keep outputs grounded to computed results only.

## API / Service Changes

1. **Planner service**
   - Update prompt and JSON schema.
   - Add tests for level selection and filters.

2. **SQL builder service**
   - Add mapping from `level` to `group_by_columns`.
   - Ensure consistent naming between actuals and targets.

3. **Inference service**
   - Add driver extraction pipeline using `gap_drivers_by_driver_levels.sql.j2`.

## Tests

1. **Unit tests**
   - Validate planner output parsing for each level.
   - Validate SQL builder renders correct `GROUP BY` and filters.

2. **Integration tests**
   - Example NL prompts:
     - "Retail SBU underperformance by zone and product drivers."
     - "North zone underperforming sales areas and product drivers."
   - Validate:
     - Correct level selection
     - SQL contains required constraints
     - Driver output is non-empty

## Rollout Steps (Order)

1. Update schema registry with hierarchy metadata.
2. Add SQL templates for level rollups and driver extraction.
3. Update planner schema + prompt.
4. Update SQL builder + validator for dynamic hierarchy groupings.
5. Extend inference logic for driver attribution.
6. Add/extend tests for planner, SQL builder, inference.

## Open Questions

- Do we have a single canonical column set for each level across both tables, or do we need explicit mapping per table?
