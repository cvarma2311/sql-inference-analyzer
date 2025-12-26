from __future__ import annotations

import logging
import os
import re
import time
import uuid
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from services.nl_planner_service.logging import setup_logging
from services.nl_planner_service.main import plan_question
from services.sql_builder_service.builder import build_query

app = FastAPI(title="NL2SQL Service", version="0.1.0")
logger = logging.getLogger("nl2sql_service")

QUERY_TEMPLATE_MAP = {
    "actual_by_level": "actual_by_level.sql.j2",
    "target_by_level": "target_by_level.sql.j2",
    "gap_drivers_by_driver_levels": "gap_drivers_by_driver_levels.sql.j2",
    "actual_by_month": "actual_by_month.sql.j2",
    "target_by_month": "target_by_month.sql.j2",
    "actual_by_zone_product": "actual_by_month.sql.j2",
    "target_by_zone_product": "target_by_month.sql.j2",
}

HIERARCHY_COLUMNS = {
    "sbu": "SBU_Name",
    "zone": "Zone_Name",
    "region": "Region_Name",
    "sales_area": "SalesArea_Name",
    "product": "ProductName",
}


class NL2SQLRequest(BaseModel):
    question: str = Field(..., min_length=3)
    sbu: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    fiscal_year: str | None = None


class SQLResult(BaseModel):
    query_id: str
    sql: str


class NL2SQLResponse(BaseModel):
    plan: dict[str, Any]
    sql: list[SQLResult]


def _get_fiscal_year(payload: NL2SQLRequest, plan: dict[str, Any]) -> str:
    if payload.fiscal_year:
        return _normalize_fiscal_year(str(payload.fiscal_year))
    plan_year = plan.get("fiscal_year")
    if plan_year:
        return _normalize_fiscal_year(str(plan_year))
    default_year = os.getenv("DEFAULT_FISCAL_YEAR")
    if default_year:
        return _normalize_fiscal_year(default_year)
    raise ValueError("fiscal_year is required (payload, plan, or DEFAULT_FISCAL_YEAR).")


def _normalize_fiscal_year(value: str) -> str:
    cleaned = value.strip()
    match = re.match(r"^(\d{4})-(\d{2})$", cleaned)
    if match:
        start_year = match.group(1)
        end_suffix = match.group(2)
        end_year = f"{start_year[:2]}{end_suffix}"
        return f"{start_year}-{end_year}"
    return cleaned


def _build_filters(plan: dict[str, Any]) -> dict[str, str]:
    filters = plan.get("filters") or {}
    if not isinstance(filters, dict):
        return {}
    sql_filters: dict[str, str] = {}
    for level_key, value in filters.items():
        column = HIERARCHY_COLUMNS.get(level_key)
        if column and value not in (None, ""):
            sql_filters[column] = str(value)
    return sql_filters


def _build_group_by_columns(level: str) -> list[str]:
    column = HIERARCHY_COLUMNS.get(level)
    return [column] if column else []


def _build_driver_columns(driver_levels: list[str]) -> list[str]:
    columns: list[str] = []
    for level in driver_levels:
        column = HIERARCHY_COLUMNS.get(level)
        if column:
            columns.append(column)
    return columns


@app.on_event("startup")
def _startup() -> None:
    load_dotenv()
    setup_logging()


@app.middleware("http")
async def log_requests(request, call_next):
    start = time.perf_counter()
    request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    request.state.request_id = request_id
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


@app.post("/nl2sql", response_model=NL2SQLResponse)
def nl2sql(payload: NL2SQLRequest, request: Request) -> NL2SQLResponse:
    try:
        request_id = getattr(request.state, "request_id", None)
        plan = plan_question(payload.question)
        fiscal_year = _get_fiscal_year(payload, plan)
        sql_filters = _build_filters(plan)
        level = plan.get("level")
        if not isinstance(level, str):
            raise ValueError("level is required in plan.")
        group_by_columns = _build_group_by_columns(level)
        if not group_by_columns:
            raise ValueError(f"Unsupported level: {level}")
        driver_levels = plan.get("driver_levels") or []
        driver_columns = _build_driver_columns(driver_levels)

        sql_results: list[SQLResult] = []
        for query_id in plan.get("queries", []):
            template_name = QUERY_TEMPLATE_MAP.get(query_id)
            if not template_name:
                raise ValueError(f"Unknown query id: {query_id}")
            context: dict[str, Any] = {}
            if query_id in ("actual_by_level", "target_by_level"):
                context = {
                    "group_by_columns": group_by_columns,
                    "filters": sql_filters,
                    "fiscal_year": fiscal_year,
                }
            elif query_id == "gap_drivers_by_driver_levels":
                if not driver_columns:
                    continue
                context = {
                    "driver_columns": driver_columns,
                    "filters": sql_filters,
                    "fiscal_year": fiscal_year,
                }
            else:
                sbu = payload.sbu or plan.get("sbu") or plan.get("filters", {}).get("sbu")
                if not sbu:
                    raise ValueError("SBU is required (from plan or request).")
                if not payload.start_date or not payload.end_date:
                    raise ValueError("start_date and end_date are required for month-based queries.")
                context = {"sbu": sbu, "start_date": payload.start_date, "end_date": payload.end_date}
            build = build_query(
                template_name,
                context,
            )
            if not build.valid:
                raise ValueError(f"SQL validation failed for {query_id}: {build.errors}")
            logger.info(
                "sql_generated",
                extra={
                    "extra": {
                        "request_id": request_id,
                        "query_id": query_id,
                        "sql": build.sql,
                    }
                },
            )
            sql_results.append(SQLResult(query_id=query_id, sql=build.sql))

        return NL2SQLResponse(plan=plan, sql=sql_results)
    except ValueError as exc:
        logger.error(
            "nl2sql_validation_failed",
            extra={"extra": {"request_id": request_id, "error": str(exc)}},
        )
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.error(
            "nl2sql_failed",
            extra={"extra": {"request_id": request_id, "error": str(exc)}},
        )
        raise HTTPException(status_code=500, detail=str(exc))
