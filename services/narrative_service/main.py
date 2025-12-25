from __future__ import annotations

import logging
import time
import uuid
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

from services.nl_planner_service.logging import setup_logging
from .service import generate_narrative

app = FastAPI(title="Narrative Service", version="0.1.0")
logger = logging.getLogger("narrative_service")


class NarrativeRequest(BaseModel):
    kpi_results: dict[str, Any]
    flags: list[str]
    context: dict[str, Any] | None = None


class NarrativeResponse(BaseModel):
    narrative: str


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


@app.post("/narrative", response_model=NarrativeResponse)
def narrative(payload: NarrativeRequest, request: Request) -> NarrativeResponse:
    try:
        request_id = getattr(request.state, "request_id", None)
        narrative_text = generate_narrative(payload.kpi_results, payload.flags, payload.context)
        logger.info(
            "narrative_generated",
            extra={"extra": {"request_id": request_id}},
        )
        return NarrativeResponse(narrative=narrative_text)
    except Exception as exc:
        logger.error(
            "narrative_failed",
            extra={"extra": {"request_id": request_id, "error": str(exc)}},
        )
        raise HTTPException(status_code=500, detail=str(exc))
