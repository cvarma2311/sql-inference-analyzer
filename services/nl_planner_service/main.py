from __future__ import annotations

import json
import re
from pathlib import Path

import logging
import time
import uuid

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from jsonschema import Draft7Validator

from .llm_client import request_plan
from .logging import setup_logging
from .prompt import PLANNER_SYSTEM_PROMPT, PLANNER_USER_TEMPLATE
from .schema_context import build_schema_context
from .settings import get_settings
from .schemas import PlanRequest, PlanResponse

BASE_DIR = Path(__file__).resolve().parents[2]
SCHEMA_PATH = BASE_DIR / "services" / "nl_planner_service" / "plan_schema.json"

app = FastAPI(title="NL Planner Service", version="0.1.0")
logger = logging.getLogger("nl_planner_service")

HIERARCHY_LEVELS = ["sbu", "zone", "region", "sales_area", "product"]
LEVEL_ALIASES = {
    "salesarea": "sales_area",
    "sales area": "sales_area",
    "sales-area": "sales_area",
}
FILTER_KEYS = {"sbu", "zone", "region", "sales_area", "product"}
DRIVER_LEVEL_MAP = {
    "sbu": ["zone", "region", "sales_area", "product"],
    "zone": ["region", "sales_area", "product"],
    "region": ["sales_area", "product"],
    "sales_area": ["product"],
    "product": [],
}


def _load_schema() -> dict:
    try:
        return json.loads(SCHEMA_PATH.read_text())
    except Exception as exc:
        raise RuntimeError(f"Failed to load schema: {exc}")


SCHEMA = _load_schema()
VALIDATOR = Draft7Validator(SCHEMA)


def plan_question(question: str) -> dict:
    settings = get_settings()
    schema_context = ""
    if settings["planner_schema_context"] != "off":
        schema_context = build_schema_context()
    schema_json = json.dumps(SCHEMA, indent=2)
    user_prompt = PLANNER_USER_TEMPLATE.format(
        question=question,
        schema_json=schema_json,
        schema_context=schema_context,
    )
    plan = request_plan(PLANNER_SYSTEM_PROMPT, user_prompt)
    if isinstance(plan, dict) and "plan" in plan and isinstance(plan["plan"], dict):
        plan = plan["plan"]
    plan = _normalize_plan(plan, question)
    errors = sorted(VALIDATOR.iter_errors(plan), key=lambda e: e.path)
    if errors:
        messages = [e.message for e in errors]
        raise ValueError("; ".join(messages))
    return plan


def _normalize_plan(plan: dict, question: str | None = None) -> dict:
    if not isinstance(plan, dict):
        return plan
    raw_level = plan.get("level")
    filters = _normalize_filters(plan.get("filters"))
    if "sbu" in plan and "sbu" not in filters:
        filters["sbu"] = str(plan["sbu"])
    question_level = _infer_level_from_question(question)
    level = question_level or _infer_level_from_filters(filters)
    if level is None:
        level = _normalize_level(raw_level)
    if level is None and isinstance(raw_level, str) and raw_level.strip():
        if "sbu" not in filters:
            filters["sbu"] = raw_level.strip()
        level = "sbu"
    if level is None:
        level = "sbu"
    plan["level"] = level
    plan["filters"] = filters
    drilldown_path = plan.get("drilldown_path")
    if isinstance(drilldown_path, list):
        normalized_path = [_normalize_level(item) for item in drilldown_path]
        normalized_path = [item for item in normalized_path if item]
        plan["drilldown_path"] = normalized_path or HIERARCHY_LEVELS
    else:
        plan["drilldown_path"] = HIERARCHY_LEVELS
    driver_levels = DRIVER_LEVEL_MAP.get(level, [])
    plan["driver_levels"] = driver_levels
    if "queries" not in plan or not isinstance(plan.get("queries"), list):
        queries = ["actual_by_level", "target_by_level"]
        if driver_levels:
            queries.append("gap_drivers_by_driver_levels")
        plan["queries"] = queries
    queries = plan.get("queries")
    if isinstance(queries, list):
        plan["queries"] = [
            _map_query_id(_normalize_query_id(q)) for q in queries if isinstance(q, str)
        ]
        if not driver_levels and "gap_drivers_by_driver_levels" in plan["queries"]:
            plan["queries"] = [
                q for q in plan["queries"] if q != "gap_drivers_by_driver_levels"
            ]
        if _has_level_queries(plan["queries"]):
            plan["queries"] = _filter_level_queries(plan["queries"])
    return plan


def _normalize_query_id(value: str) -> str:
    normalized = value.strip().lower()
    normalized = re.sub(r"\\s*_*\\s+", "_", normalized)
    normalized = re.sub(r"[^a-z0-9_]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized


def _map_query_id(value: str) -> str:
    mapping = {
        "actual_by_level": "actual_by_level",
        "target_by_level": "target_by_level",
        "gap_drivers_by_driver_levels": "gap_drivers_by_driver_levels",
        "gap_drivers": "gap_drivers_by_driver_levels",
        "drivers_by_level": "gap_drivers_by_driver_levels",
        "drivers": "gap_drivers_by_driver_levels",
        "actual_tmt": "actual_by_month",
        "actual_sales": "actual_by_month",
        "actual_weight_tmt": "actual_by_month",
        "target_tmt": "target_by_month",
        "target_sales": "target_by_month",
        "target_weight_tmt": "target_by_month",
        "actual_by_zone": "actual_by_zone_product",
        "actual_by_product": "actual_by_zone_product",
        "actual_by_zone_product": "actual_by_zone_product",
        "target_by_zone": "target_by_zone_product",
        "target_by_product": "target_by_zone_product",
        "target_by_zone_product": "target_by_zone_product",
        "actual_by_month": "actual_by_month",
        "target_by_month": "target_by_month",
    }
    return mapping.get(value, value)


def _normalize_level(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower().replace("-", " ").replace("_", " ")
    normalized = " ".join(normalized.split())
    normalized = LEVEL_ALIASES.get(normalized, normalized)
    normalized = normalized.replace(" ", "_")
    return normalized if normalized in HIERARCHY_LEVELS else None


def _normalize_filters(filters: dict | None) -> dict[str, str]:
    if not isinstance(filters, dict):
        return {}
    normalized: dict[str, str] = {}
    for key, value in filters.items():
        if not isinstance(key, str):
            continue
        level_key = _normalize_level(key) or key.strip().lower().replace("-", "_")
        if level_key in FILTER_KEYS and value not in (None, ""):
            normalized[level_key] = str(value)
    return normalized


def _infer_level_from_filters(filters: dict[str, str]) -> str | None:
    for level in reversed(HIERARCHY_LEVELS):
        if level in filters:
            return level
    return None


def _infer_level_from_question(question: str | None) -> str | None:
    if not question:
        return None
    lowered = question.lower()
    if "product" in lowered:
        return "product"
    if "sales area" in lowered or "salesarea" in lowered:
        return "sales_area"
    if "region" in lowered:
        return "region"
    if "zone" in lowered:
        return "zone"
    if "sbu" in lowered:
        return "sbu"
    return None


def _driver_levels_from_question(level: str, question: str | None) -> list[str]:
    if not question:
        return DRIVER_LEVEL_MAP.get(level, [])
    lowered = question.lower()
    mentions = []
    for item in HIERARCHY_LEVELS:
        if item == level:
            continue
        keyword = item.replace("_", " ")
        if keyword in lowered or item in lowered:
            mentions.append(item)
    if mentions:
        ordered = [item for item in HIERARCHY_LEVELS if item in mentions]
        return [item for item in ordered if HIERARCHY_LEVELS.index(item) > HIERARCHY_LEVELS.index(level)]
    return DRIVER_LEVEL_MAP.get(level, [])


def _has_level_queries(queries: list[str]) -> bool:
    return any(q in {"actual_by_level", "target_by_level", "gap_drivers_by_driver_levels"} for q in queries)


def _filter_level_queries(queries: list[str]) -> list[str]:
    allow = {"actual_by_level", "target_by_level", "gap_drivers_by_driver_levels"}
    return [q for q in queries if q in allow]




@app.on_event("startup")
def _startup() -> None:
    load_dotenv()
    setup_logging()


@app.middleware("http")
async def log_requests(request, call_next):
    start = time.perf_counter()
    request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    duration_ms = round((time.perf_counter() - start) * 1000, 2)
    logger.info(
        "request_completed",
        extra={
            "extra": {
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "duration_ms": duration_ms,
            }
        },
    )
    return response


@app.post("/plan", response_model=PlanResponse)
def create_plan(payload: PlanRequest) -> PlanResponse:
    try:
        plan = plan_question(payload.question)
        settings = get_settings()
        if settings["log_plan_output"] != "off":
            logger.info(
                "plan_output",
                extra={"extra": {"plan": plan}},
            )
        return PlanResponse(**plan)
    except ValueError as exc:
        logger.error("plan_validation_failed", extra={"extra": {"error": str(exc)}})
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.error("plan_request_failed", extra={"extra": {"error": str(exc)}})
        raise HTTPException(status_code=500, detail=str(exc))
