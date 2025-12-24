from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .template_renderer import render_template
from .validator import SqlValidator


@dataclass
class BuildResult:
    sql: str
    valid: bool
    errors: list[str]


def build_query(template_name: str, context: dict[str, Any]) -> BuildResult:
    sql = render_template(template_name, context)
    validator = SqlValidator()
    result = validator.validate(sql)
    return BuildResult(sql=sql, valid=result.valid, errors=result.errors)
