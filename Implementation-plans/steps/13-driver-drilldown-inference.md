# Step 13: Driver Drilldown Inference (Zone -> Region -> Sales Area -> Product)

## Objective

Populate underperforming/overperforming drilldown drivers from real query results so the narrative JSON is data-backed.

## Inputs

- `level`, `filters`, `driver_levels`, `fiscal_year` from planner.
- Existing hierarchy columns: `SBU_Name`, `Zone_Name`, `Region_Name`, `SalesArea_Name`, `ProductName`.

## Strategy

1. **Driver query alignment**
   - Ensure `gap_drivers_by_driver_levels.sql.j2` uses `driver_levels` derived from the hierarchy:
     - `sbu` -> `zone, region`
     - `zone` -> `region, sales_area`
     - `region` -> `sales_area, product`
     - `sales_area` -> `sales_area, product`
     - `product` -> `zone, region, sales_area` (keep current if the user asked “product drivers”)
   - If the user explicitly asks for a deeper level, include it in `driver_levels`.

2. **Execution result parsing**
   - Consume `gap_drivers_by_driver_levels` rows.
   - Compute `gap_tmt` per row and contribution vs total gap.
   - Split into top 5 negative and top 5 positive.

3. **Drilldown tree construction**
   - Build a nested tree using `driver_levels` order.
   - Each node: `name`, `gap_tmt`, `contribution_pct`, `children`.
   - Return tree under `narrative.drivers.underperforming.drilldown` and `overperforming.drilldown`.

4. **Fallback rules**
   - If driver query returns zero rows:
     - Include an empty drilldown but keep `summary` and `context`.
     - Log a warning with `driver_levels` and filters.
   - Optionally add a secondary query for `actual_by_level` and `target_by_level` at `driver_levels` if needed.

## Output Shape (Structured Narrative)

```json
{
  "summary": { "achievement_pct": 68.07, "gap_tmt": -6025.35, "flags": ["UNDERPERFORMANCE"] },
  "context": {
    "level": "product",
    "filters": { "sbu": "Retail" },
    "fiscal_year": "2025-26",
    "driver_levels": ["zone", "region", "sales_area"]
  },
  "drivers": {
    "underperforming": {
      "top": [{ "zone": "South", "region": "TN", "sales_area": "Chennai", "gap_tmt": -120.4, "contribution_pct": 12.3 }],
      "drilldown": [
        { "name": "South", "gap_tmt": -320.1, "contribution_pct": 33.1, "children": [
          { "name": "TN", "gap_tmt": -180.2, "contribution_pct": 18.6, "children": [
            { "name": "Chennai", "gap_tmt": -120.4, "contribution_pct": 12.3, "children": [] }
          ]}
        ]}
      ]
    },
    "overperforming": { "top": [...], "drilldown": [...] }
  }
}
```

## Acceptance Criteria

- Drilldown drivers are populated with non-empty arrays when data exists.
- Underperforming and overperforming sections reflect the correct sign of `gap_tmt`.
- Drilldown hierarchy matches `driver_levels` for the current query context.
