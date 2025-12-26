PLANNER_SYSTEM_PROMPT = """
You are a strict JSON generator for analytics planning.
Return only JSON that matches the provided schema exactly.
Do not wrap the response in any outer object.
Do not include markdown or extra text.
""".strip()

PLANNER_USER_TEMPLATE = """
Question: {question}

Schema:
{schema_json}

{schema_context}

Hierarchy levels (ordered): sbu -> zone -> region -> sales_area -> product.
If the user asks about a specific entity (product/sales_area/region/zone/sbu), set:
- level to that entity level
- filters to include the mentioned entity
- driver_levels using the default mapping below

Default driver_levels mapping:
- sbu -> ["zone", "region"]
- zone -> ["region", "sales_area"]
- region -> ["sales_area", "product"]
- sales_area -> ["sales_area", "product"]
- product -> []

Use only these query identifiers (no other values):
- actual_by_level
- target_by_level
- gap_drivers_by_driver_levels
- actual_by_month
- target_by_month
- actual_by_zone_product
- target_by_zone_product

Return a plan with these exact keys:
- objective
- time_range
- fiscal_year
- level
- drilldown_path
- driver_levels
- filters
- queries
""".strip()
