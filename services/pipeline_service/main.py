from __future__ import annotations

import logging
import re
import os
import time
import uuid
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from services.audit_log_service.audit_log import build_audit_record, write_audit_record
from services.inference_service.rules import run_inference
from services.narrative_service.service import generate_bullet_summary
from services.nl_planner_service.logging import setup_logging
from services.nl_planner_service.main import plan_question
from services.sql_builder_service.builder import build_query
from services.sql_executor_service.executor import execute_queries

app = FastAPI(title="NL2SQL Pipeline", version="0.1.0")
logger = logging.getLogger("pipeline_service")

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
COLUMN_TO_LEVEL = {value: key for key, value in HIERARCHY_COLUMNS.items()}


class PipelineRequest(BaseModel):
    question: str = Field(..., min_length=3)
    start_date: str | None = None
    end_date: str | None = None
    fiscal_year: str | None = None


class PipelineResponse(BaseModel):
    plan: dict[str, Any]
    sql: list[dict[str, Any]]
    execution: list[dict[str, Any]]
    inference: dict[str, Any]
    narrative: dict[str, Any]
    summary_bullets: list[str]
    audit: dict[str, Any]


@app.on_event("startup")
def _startup() -> None:
    load_dotenv()
    setup_logging()


@app.middleware("http")
async def log_requests(request: Request, call_next):
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


def _get_default_dates(payload: PipelineRequest) -> tuple[str, str]:
    if payload.start_date and payload.end_date:
        return payload.start_date, payload.end_date
    start_date = os.getenv("DEFAULT_START_DATE")
    end_date = os.getenv("DEFAULT_END_DATE")
    if not start_date or not end_date:
        raise ValueError("DEFAULT_START_DATE and DEFAULT_END_DATE must be set for pipeline execution.")
    return start_date, end_date


def _get_fiscal_year(payload: PipelineRequest, plan: dict[str, Any]) -> str:
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


def _extract_totals(execution_results: list[dict[str, Any]]) -> tuple[float, float, list[float]]:
    actual_total = 0.0
    target_total = 0.0
    trend_values: list[float] = []

    for result in execution_results:
        query_id = result.get("query_id")
        rows = result.get("rows", [])
        if query_id == "actual_by_month":
            for row in rows:
                value = row.get("actual_tmt")
                if value is not None:
                    actual_total += float(value)
                    trend_values.append(float(value))
        if query_id == "actual_by_level":
            for row in rows:
                value = row.get("actual_tmt")
                if value is not None:
                    actual_total += float(value)
        if query_id in ("target_by_month", "target_by_level"):
            for row in rows:
                value = row.get("target_tmt")
                if value is not None:
                    target_total += float(value)

    return actual_total, target_total, trend_values


def _extract_driver_rows(execution_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for result in execution_results:
        if result.get("query_id") == "gap_drivers_by_driver_levels":
            rows = result.get("rows", [])
            return rows if isinstance(rows, list) else []
    return []


def _build_driver_lists(
    rows: list[dict[str, Any]],
    driver_columns: list[str],
    total_gap: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not rows or not driver_columns:
        return [], []
    total_gap_abs = abs(total_gap)

    def _row_payload(row: dict[str, Any]) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        for col in driver_columns:
            level_key = COLUMN_TO_LEVEL.get(col, col)
            payload[level_key] = row.get(col)
        gap_value = float(row.get("gap_tmt", 0) or 0)
        payload["gap_tmt"] = gap_value
        payload["contribution_pct"] = (
            round(abs(gap_value) / total_gap_abs * 100, 2) if total_gap_abs else 0.0
        )
        return payload

    negative = [row for row in rows if (row.get("gap_tmt") or 0) < 0]
    positive = [row for row in rows if (row.get("gap_tmt") or 0) > 0]

    negative_sorted = sorted(negative, key=lambda r: float(r.get("gap_tmt", 0)))
    positive_sorted = sorted(positive, key=lambda r: float(r.get("gap_tmt", 0)), reverse=True)

    top_negative = [_row_payload(row) for row in negative_sorted[:5]]
    top_positive = [_row_payload(row) for row in positive_sorted[:5]]
    return top_negative, top_positive


def _normalize_driver_rows(
    rows: list[dict[str, Any]],
    driver_columns: list[str],
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    if not rows:
        return normalized
    for row in rows:
        payload: dict[str, Any] = {}
        for col in driver_columns:
            level_key = COLUMN_TO_LEVEL.get(col, col)
            payload[level_key] = row.get(col)
        payload["gap_tmt"] = row.get("gap_tmt")
        normalized.append(payload)
    return normalized


def _build_structured_narrative(
    inference_payload: dict[str, Any],
    actual_total: float,
    target_total: float,
    driver_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    driver_levels = inference_payload.get("driver_levels", [])
    negative = inference_payload.get("top_negative_drivers", [])
    positive = inference_payload.get("top_positive_drivers", [])
    total_gap = float(inference_payload.get("gap_tmt") or 0.0)

    def _build_driver_tree(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows or not driver_levels:
            return []
        total_gap_abs = abs(total_gap)
        tree: dict[str, dict[str, Any]] = {}
        for row in rows:
            current = tree
            for level in driver_levels:
                name = row.get(level)
                if name is None:
                    break
                node = current.get(name)
                if node is None:
                    node = {"name": name, "gap_tmt": 0.0, "children": {}}
                    current[name] = node
                gap_value = float(row.get("gap_tmt") or 0.0)
                node["gap_tmt"] += gap_value
                current = node["children"]

        def _finalize(nodes: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
            items: list[dict[str, Any]] = []
            for node in nodes.values():
                gap_value = float(node.get("gap_tmt") or 0.0)
                payload = {
                    "name": node.get("name"),
                    "gap_tmt": round(gap_value, 3),
                    "contribution_pct": round(abs(gap_value) / total_gap_abs * 100, 2)
                    if total_gap_abs
                    else 0.0,
                    "children": _finalize(node.get("children", {})),
                }
                items.append(payload)
            return items

        return _finalize(tree)

    under_rows = [row for row in driver_rows if float(row.get("gap_tmt") or 0.0) < 0]
    over_rows = [row for row in driver_rows if float(row.get("gap_tmt") or 0.0) > 0]

    return {
        "summary": {
            "achievement_pct": inference_payload.get("achievement_pct"),
            "gap_tmt": inference_payload.get("gap_tmt"),
            "flags": inference_payload.get("flags", []),
            "actual_tmt": round(actual_total, 3),
            "target_tmt": round(target_total, 3),
        },
        "context": {
            "level": inference_payload.get("level"),
            "filters": inference_payload.get("filters", {}),
            "fiscal_year": inference_payload.get("fiscal_year"),
            "driver_levels": inference_payload.get("driver_levels", []),
        },
        "drivers": {
            "underperforming": {
                "top": negative,
                "drilldown": _build_driver_tree(under_rows),
            },
            "overperforming": {
                "top": positive,
                "drilldown": _build_driver_tree(over_rows),
            },
        },
    }


def _build_summary_drivers(narrative: dict[str, Any]) -> dict[str, Any]:
    drivers = narrative.get("drivers", {})
    under = drivers.get("underperforming", {})
    over = drivers.get("overperforming", {})
    return {
        "underperforming_top": under.get("top", []),
        "overperforming_top": over.get("top", []),
    }


@app.post("/analyze", response_model=PipelineResponse)
def analyze(payload: PipelineRequest, request: Request) -> PipelineResponse:
    try:
        request_id = getattr(request.state, "request_id", None)
        plan = plan_question(payload.question)
        fiscal_year = _get_fiscal_year(payload, plan)
        sql_filters = _build_filters(plan)
        level = plan.get("level")
        if not isinstance(level, str):
            raise ValueError("level is required in plan.")
        driver_levels = plan.get("driver_levels") or []
        group_by_columns = _build_group_by_columns(level)
        if not group_by_columns:
            raise ValueError(f"Unsupported level: {level}")
        driver_columns = _build_driver_columns(driver_levels)
        start_date = end_date = None
        if any(q in ("actual_by_month", "target_by_month") for q in plan.get("queries", [])):
            start_date, end_date = _get_default_dates(payload)

        sql_outputs: list[dict[str, Any]] = []
        log_sql = os.getenv("LOG_SQL_OUTPUT", "on").lower() != "off"
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
                sbu = plan.get("sbu") or plan.get("filters", {}).get("sbu")
                if not sbu:
                    raise ValueError("SBU is required for month-based queries.")
                if not start_date or not end_date:
                    raise ValueError("start_date and end_date are required for month-based queries.")
                context = {"sbu": sbu, "start_date": start_date, "end_date": end_date}
            build = build_query(
                template_name,
                context,
            )
            if log_sql:
                logger.info(
                    "sql_generated",
                    extra={"extra": {"request_id": request_id, "query_id": query_id, "sql": build.sql}},
                )
            if not build.valid:
                logger.error(
                    "sql_validation_failed",
                    extra={
                        "extra": {
                            "request_id": request_id,
                            "query_id": query_id,
                            "errors": build.errors,
                            "sql": build.sql,
                        }
                    },
                )
                raise ValueError(f"SQL validation failed for {query_id}: {build.errors}")
            sql_outputs.append({"query_id": query_id, "sql": build.sql})

        execution_results = execute_queries([item["sql"] for item in sql_outputs], request_id=request_id)
        execution_payload = []
        for query, result in zip(sql_outputs, execution_results, strict=False):
            execution_payload.append(
                {
                    "query_id": query["query_id"],
                    "row_count": result.row_count,
                    "execution_ms": result.execution_ms,
                    "result_hash": result.result_hash,
                    "rows": result.rows,
                }
            )

        actual_total, target_total, trend_values = _extract_totals(execution_payload)
        inference = run_inference(actual_total, target_total, trend_values=trend_values)
        driver_rows = _extract_driver_rows(execution_payload)
        top_negative, top_positive = _build_driver_lists(driver_rows, driver_columns, inference.gap_tmt)
        flags = list(inference.flags)
        if target_total == 0:
            flags = [flag for flag in flags if flag != "UNDERPERFORMANCE"]
            flags.append("NO_TARGET")
        inference_payload = {
            "level": level,
            "filters": plan.get("filters", {}),
            "fiscal_year": fiscal_year,
            "achievement_pct": inference.achievement_pct,
            "gap_tmt": inference.gap_tmt,
            "driver_levels": driver_levels,
            "top_negative_drivers": top_negative,
            "top_positive_drivers": top_positive,
            "flags": flags,
        }

        narrative = _build_structured_narrative(
            inference_payload,
            actual_total,
            target_total,
            _normalize_driver_rows(driver_rows, driver_columns),
        )
        summary_bullets: list[str] = []
        try:
            summary_bullets = generate_bullet_summary(
                kpi_results=narrative.get("summary", {}),
                flags=inference_payload.get("flags", []),
                context=narrative.get("context", {}),
                drivers=_build_summary_drivers(narrative),
            )
        except Exception as exc:
            logger.error(
                "summary_bullets_failed",
                extra={"extra": {"request_id": request_id, "error": str(exc)}},
            )

        audit_record = build_audit_record(
            request_id=request_id or str(uuid.uuid4()),
            question=payload.question,
            plan=plan,
            sql=sql_outputs,
            execution=execution_payload,
            inference=inference_payload,
            narrative=narrative,
            summary_bullets=summary_bullets,
        )
        audit_info = write_audit_record(audit_record)

        return PipelineResponse(
            plan=plan,
            sql=sql_outputs,
            execution=execution_payload,
            inference=inference_payload,
            narrative=narrative,
            summary_bullets=summary_bullets,
            audit=audit_info,
        )
    except ValueError as exc:
        logger.error(
            "pipeline_validation_failed",
            extra={"extra": {"request_id": request_id, "error": str(exc)}},
        )
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.error(
            "pipeline_failed",
            extra={"extra": {"request_id": request_id, "error": str(exc)}},
        )
        raise HTTPException(status_code=500, detail=str(exc))
