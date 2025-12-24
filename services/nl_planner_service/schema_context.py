from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

BASE_DIR = Path(__file__).resolve().parents[2]
CONFIG_DIR = BASE_DIR / "config"


def _load_yaml(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text())
    if not isinstance(data, dict):
        raise ValueError(f"Invalid YAML at {path}")
    return data


def build_schema_context() -> str:
    kpis = _load_yaml(CONFIG_DIR / "kpi_registry.yaml")
    schema = _load_yaml(CONFIG_DIR / "schema_registry.yaml")

    lines: list[str] = []
    lines.append("KPI Registry:")
    for kpi_name, kpi_def in kpis.items():
        desc = kpi_def.get("description", "")
        lines.append(
            f"- {kpi_name}: {kpi_def.get('table')}.{kpi_def.get('column')} ({kpi_def.get('aggregation')}) {desc}".strip()
        )

    lines.append("\nSchema Registry:")
    for table, table_def in schema.get("tables", {}).items():
        lines.append(f"- {table}:")
        descriptions = table_def.get("column_descriptions", {})
        for col in table_def.get("allowed_columns", []):
            desc = descriptions.get(col, "")
            if desc:
                lines.append(f"  - {col}: {desc}")
            else:
                lines.append(f"  - {col}")

    return "\n".join(lines)
