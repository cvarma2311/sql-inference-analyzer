from __future__ import annotations

import logging
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
    "actual_by_month": "actual_by_month.sql.j2",
    "target_by_month": "target_by_month.sql.j2",
    "actual_by_zone_product": "actual_by_month.sql.j2",
    "target_by_zone_product": "target_by_month.sql.j2",
}


class NL2SQLRequest(BaseModel):
    question: str = Field(..., min_length=3)
    sbu: str | None = None
    start_date: str
    end_date: str


class SQLResult(BaseModel):
    query_id: str
    sql: str


class NL2SQLResponse(BaseModel):
    plan: dict[str, Any]
    sql: list[SQLResult]


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
        sbu = payload.sbu or plan.get("sbu")
        if not sbu:
            raise ValueError("SBU is required (from plan or request).")

        sql_results: list[SQLResult] = []
        for query_id in plan.get("queries", []):
            template_name = QUERY_TEMPLATE_MAP.get(query_id)
            if not template_name:
                raise ValueError(f"Unknown query id: {query_id}")
            build = build_query(
                template_name,
                {
                    "sbu": sbu,
                    "start_date": payload.start_date,
                    "end_date": payload.end_date,
                },
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
