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

Use only these query identifiers (no other values):
- actual_by_month
- target_by_month
- actual_by_zone_product
- target_by_zone_product

Return a plan with these exact keys:
- objective
- sbu
- time_range
- breakdowns
- queries
""".strip()
