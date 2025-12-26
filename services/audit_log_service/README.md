# Audit Log Service

## Goal
Persist request artifacts for traceability.

## Storage

- JSON file per request (default directory: `audit_logs/`).

## Environment

- `AUDIT_LOG_DIR` (default: `audit_logs`)

## Local Usage

```bash
python -c "from services.audit_log_service.audit_log import build_audit_record, write_audit_record; rec = build_audit_record('req-1','question',{},[],[],{}, {'summary':{}}, ['bullet 1']); print(write_audit_record(rec))"
```
