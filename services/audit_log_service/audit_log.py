from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


@dataclass
class AuditRecord:
    request_id: str
    question: str
    plan: dict[str, Any]
    sql: list[dict[str, Any]]
    execution: list[dict[str, Any]]
    inference: dict[str, Any]
    narrative: str
    created_at: str


def _hash_payload(payload: Any) -> str:
    data = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def _get_audit_dir() -> Path:
    load_dotenv()
    base_dir = os.getenv("AUDIT_LOG_DIR", "audit_logs")
    path = Path(base_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_audit_record(record: AuditRecord) -> dict[str, Any]:
    audit_dir = _get_audit_dir()
    payload = {
        "request_id": record.request_id,
        "question": record.question,
        "plan": record.plan,
        "sql": record.sql,
        "execution": record.execution,
        "inference": record.inference,
        "narrative": record.narrative,
        "created_at": record.created_at,
    }
    record_hash = _hash_payload(payload)
    payload["record_hash"] = record_hash

    file_path = audit_dir / f"{record.request_id}.json"
    file_path.write_text(json.dumps(payload, indent=2))

    return {
        "path": str(file_path),
        "record_hash": record_hash,
    }


def build_audit_record(
    request_id: str,
    question: str,
    plan: dict[str, Any],
    sql: list[dict[str, Any]],
    execution: list[dict[str, Any]],
    inference: dict[str, Any],
    narrative: str,
) -> AuditRecord:
    created_at = datetime.now(timezone.utc).isoformat()
    return AuditRecord(
        request_id=request_id,
        question=question,
        plan=plan,
        sql=sql,
        execution=execution,
        inference=inference,
        narrative=narrative,
        created_at=created_at,
    )
