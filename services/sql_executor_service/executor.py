from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Iterable

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

logger = logging.getLogger("sql_executor_service")


@dataclass
class ExecutionResult:
    sql: str
    row_count: int
    execution_ms: float
    result_hash: str
    rows: list[dict[str, Any]]


def _build_database_url() -> str:
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return database_url

    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    sslmode = os.getenv("DB_SSLMODE")

    if not all([host, port, name, user, password]):
        raise RuntimeError("DATABASE_URL is not set and DB_* params are incomplete.")

    database_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"
    if sslmode:
        database_url = f"{database_url}?sslmode={sslmode}"
    return database_url


def _hash_rows(rows: list[dict[str, Any]]) -> str:
    payload = json.dumps(rows, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def execute_queries(
    sql_statements: Iterable[str],
    max_rows: int | None = None,
    request_id: str | None = None,
) -> list[ExecutionResult]:
    load_dotenv()
    database_url = _build_database_url()
    engine = create_engine(database_url, pool_pre_ping=True)

    results: list[ExecutionResult] = []
    max_rows = max_rows if max_rows is not None else int(os.getenv("SQL_MAX_ROWS", "1000"))
    log_sql = os.getenv("LOG_SQL_OUTPUT", "on").lower() != "off"

    with engine.connect() as conn:
        for sql in sql_statements:
            if log_sql:
                logger.info(
                    "sql_execute_start",
                    extra={"extra": {"sql": sql, "request_id": request_id}},
                )
            start = time.perf_counter()
            result = conn.execute(text(sql))
            rows = [dict(row._mapping) for row in result.fetchmany(max_rows)]
            duration_ms = round((time.perf_counter() - start) * 1000, 2)
            result_hash = _hash_rows(rows)
            if log_sql:
                logger.info(
                    "sql_execute_done",
                    extra={
                        "extra": {
                            "request_id": request_id,
                            "row_count": len(rows),
                            "execution_ms": duration_ms,
                            "result_hash": result_hash,
                        }
                    },
                )
            results.append(
                ExecutionResult(
                    sql=sql,
                    row_count=len(rows),
                    execution_ms=duration_ms,
                    result_hash=result_hash,
                    rows=rows,
                )
            )

    return results
