from __future__ import annotations

import logging
import os
import time
import uuid
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from services.audit_log_service.audit_log import build_audit_record, write_audit_record
from services.inference_service.rules import run_inference
from services.narrative_service.service import generate_narrative
from services.nl_planner_service.logging import setup_logging
from services.nl_planner_service.main import plan_question
from services.sql_builder_service.builder import build_query
from services.sql_executor_service.executor import execute_queries

app = FastAPI(title="NL2SQL Pipeline", version="0.1.0")
logger = logging.getLogger("pipeline_service")

QUERY_TEMPLATE_MAP = {
    "actual_by_month": "actual_by_month.sql.j2",
    "target_by_month": "target_by_month.sql.j2",
    "actual_by_zone_product": "actual_by_month.sql.j2",
    "target_by_zone_product": "target_by_month.sql.j2",
}


class PipelineRequest(BaseModel):
    question: str = Field(..., min_length=3)
    start_date: str | None = None
    end_date: str | None = None


class PipelineResponse(BaseModel):
    plan: dict[str, Any]
    sql: list[dict[str, Any]]
    execution: list[dict[str, Any]]
    inference: dict[str, Any]
    narrative: str
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
        if query_id == "target_by_month":
            for row in rows:
                value = row.get("target_tmt")
                if value is not None:
                    target_total += float(value)

    return actual_total, target_total, trend_values


@app.post("/analyze", response_model=PipelineResponse)
def analyze(payload: PipelineRequest, request: Request) -> PipelineResponse:
    try:
        request_id = getattr(request.state, "request_id", None)
        plan = plan_question(payload.question)
        sbu = plan.get("sbu")
        if not sbu:
            raise ValueError("SBU is required in plan.")

        start_date, end_date = _get_default_dates(payload)

        sql_outputs: list[dict[str, Any]] = []
        log_sql = os.getenv("LOG_SQL_OUTPUT", "on").lower() != "off"
        for query_id in plan.get("queries", []):
            template_name = QUERY_TEMPLATE_MAP.get(query_id)
            if not template_name:
                raise ValueError(f"Unknown query id: {query_id}")
            build = build_query(
                template_name,
                {"sbu": sbu, "start_date": start_date, "end_date": end_date},
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
        inference_payload = {
            "achievement_pct": inference.achievement_pct,
            "gap_tmt": inference.gap_tmt,
            "flags": inference.flags,
        }

        narrative = generate_narrative(inference_payload, inference.flags)

        audit_record = build_audit_record(
            request_id=request_id or str(uuid.uuid4()),
            question=payload.question,
            plan=plan,
            sql=sql_outputs,
            execution=execution_payload,
            inference=inference_payload,
            narrative=narrative,
        )
        audit_info = write_audit_record(audit_record)

        return PipelineResponse(
            plan=plan,
            sql=sql_outputs,
            execution=execution_payload,
            inference=inference_payload,
            narrative=narrative,
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
