from __future__ import annotations

from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, StrictUndefined

import os

from .registry_loader import load_mandatory_constraints

BASE_DIR = Path(__file__).resolve().parents[2]
TEMPLATE_DIR = BASE_DIR / "config" / "query_templates"


env = Environment(
    loader=FileSystemLoader(str(TEMPLATE_DIR)),
    undefined=StrictUndefined,
    autoescape=False,
    trim_blocks=True,
    lstrip_blocks=True,
)


def render_template(template_name: str, context: dict[str, Any]) -> str:
    template = env.get_template(template_name)
    mandatory_constraints = load_mandatory_constraints()
    db_schema = os.getenv("DB_SCHEMA", "public")
    merged_context = {**context, "mandatory_constraints": mandatory_constraints, "db_schema": db_schema}
    return template.render(merged_context)
