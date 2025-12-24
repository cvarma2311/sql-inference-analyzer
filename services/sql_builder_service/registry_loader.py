from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

BASE_DIR = Path(__file__).resolve().parents[2]
CONFIG_DIR = BASE_DIR / "config"


def load_yaml(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text())
    if not isinstance(data, dict):
        raise ValueError(f"Invalid YAML at {path}")
    return data


def load_kpi_registry() -> dict[str, Any]:
    return load_yaml(CONFIG_DIR / "kpi_registry.yaml")


def load_schema_registry() -> dict[str, Any]:
    return load_yaml(CONFIG_DIR / "schema_registry.yaml")


def load_mandatory_constraints() -> str:
    return (CONFIG_DIR / "rules" / "mandatory_constraints.sql").read_text().strip()
