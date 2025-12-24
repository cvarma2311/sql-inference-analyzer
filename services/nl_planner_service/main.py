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
    plan = _normalize_plan(plan)
    errors = sorted(VALIDATOR.iter_errors(plan), key=lambda e: e.path)
    if errors:
        messages = [e.message for e in errors]
        raise ValueError("; ".join(messages))
    return plan


def _normalize_plan(plan: dict) -> dict:
    if not isinstance(plan, dict):
        return plan
    queries = plan.get("queries")
    if isinstance(queries, list):
        plan["queries"] = [
            _map_query_id(_normalize_query_id(q)) for q in queries if isinstance(q, str)
        ]
    return plan


def _normalize_query_id(value: str) -> str:
    normalized = value.strip().lower()
    normalized = re.sub(r"\\s*_*\\s+", "_", normalized)
    normalized = re.sub(r"[^a-z0-9_]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized


def _map_query_id(value: str) -> str:
    mapping = {
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
        return PlanResponse(**plan)
    except ValueError as exc:
        logger.error("plan_validation_failed", extra={"extra": {"error": str(exc)}})
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.error("plan_request_failed", extra={"extra": {"error": str(exc)}})
        raise HTTPException(status_code=500, detail=str(exc))
