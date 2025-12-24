# Offline NL to Multi-SQL Inference Platform

Detailed Implementation Plan (v1)

## 1. Scope and Assumptions

### Scope (v1 - locked)

- Database: PostgreSQL
- Tables:
  - MOM_DAY_LEVEL_DATA (Actual Sales)
  - M60_LEVEL_METADATA (Target Sales)
- Queries: NL to multiple SQL queries
- Deterministic SQL only
- Models: Offline only
- No external APIs
- Output:
  - Generated SQLs
  - SQL results
  - Explainable reasoning trace
  - Inference narrative (grounded)

### Explicit Constraints

- SQL is always executed first
- LLM never calculates metrics
- LLM never accesses DB
- Chain-of-thought is not exposed
- Reasoning trace is explicit and structured

## 2. High-Level System Components

```
/services
  ├── nl_planner_service
  ├── sql_builder_service
  ├── sql_executor_service
  ├── inference_service
  └── audit_log_service

/models
  ├── llm/
  └── embeddings/ (optional)

/config
  ├── schema_registry.yaml
  ├── kpi_registry.yaml
  ├── query_templates/
  └── rules/
```

## 3. Technology Stack (Final)

### Runtime

- Python 3.10+
- FastAPI
- Pydantic
- SQLAlchemy / psycopg2

### Database

- PostgreSQL (existing)

### LLM Runtime (Offline)

Choose one:

- llama.cpp (CPU / small GPU)
- vLLM (GPU preferred)
- Text Generation Inference (TGI)

Recommendation for v1: llama.cpp (simplest, CPU-friendly)

## 4. Offline Models (Exactly What to Use)

### 4.1 Primary LLM (Planning + Narrative)

- Model class: LLaMA / Mistral / Qwen family
- Size: 7B or 8B
- Quantized (GGUF) preferred

Recommended model (v1):

- Llama-3-8B-Instruct-GGUF

This single model is sufficient for v1.

### 4.2 Embeddings Model (Optional but Recommended)

Used for:

- Mapping NL terms to ProductName / SBU / synonyms

Model:

- bge-small-en-v1.5 or all-MiniLM-L6-v2

## 5. Model Download and Setup (Step-by-Step)

### 5.1 Install llama.cpp

```bash
git clone https://github.com/ggerganov/llama.cpp
cd llama.cpp
make
```

### 5.2 Download LLM Model (Example: Llama-3-8B-Instruct-GGUF)

```bash
mkdir -p models/llm
cd models/llm

wget https://huggingface.co/lmstudio-community/Meta-Llama-3-8B-Instruct-GGUF/resolve/main/Meta-Llama-3-8B-Instruct-Q4_K_M.gguf
```

### 5.3 Run Model Locally

```bash
./llama.cpp/server \
  -m models/llm/Meta-Llama-3-8B-Instruct-Q4_K_M.gguf \
  --port 8001 \
  --ctx-size 8192
```

This exposes:

- `POST http://localhost:8001/v1/chat/completions`

### 5.4 Optional Embedding Model Setup

```bash
pip install sentence-transformers
```

```python
from sentence_transformers import SentenceTransformer
model = SentenceTransformer("all-MiniLM-L6-v2")
```

## 6. Service-by-Service Implementation Details

### 6.1 NL Planner Service

Purpose:

- Convert NL to structured analysis plan

Input:

```json
{
  "question": "How is Retail doing this FY vs target and what are the drivers?"
}
```

Output (strict JSON):

```json
{
  "objective": "actual_vs_target",
  "sbu": "Retail",
  "time_range": "current_fy_to_date",
  "breakdowns": ["month", "zone", "product"],
  "queries": [
    "actual_by_month",
    "target_by_month",
    "actual_by_zone_product",
    "target_by_zone_product"
  ]
}
```

Implementation:

- FastAPI endpoint
- LLM prompt forces JSON schema
- Validate with jsonschema

### 6.2 KPI Registry (`kpi_registry.yaml`)

```yaml
actual_tmt:
  table: MOM_DAY_LEVEL_DATA
  column: NETWEIGHT_TMT
  aggregation: SUM

target_tmt:
  table: M60_LEVEL_METADATA
  column: TARGET_QTY_TMT
  aggregation: SUM
```

### 6.3 Schema Registry (`schema_registry.yaml`)

Schema source: `artifacts/Sales Schema and Parameters.pdf` (V1 uses only MOM_DAY_LEVEL_DATA and M60_LEVEL_METADATA).

```yaml
tables:
  MOM_DAY_LEVEL_DATA:
    allowed_columns:
      - ORGSBUCD
      - SBU_Name
      - ORGZONECD
      - Zone_Name
      - ORGROCD
      - Region_Name
      - ORGSACD
      - SalesArea_Name
      - PRODUCTCODE
      - ProductName
      - CURFISCALYEAR
      - FISCALYEAR
      - YEARMONTH
      - NETWEIGHT_UOM
      - NETWEIGHT_KG
      - NETWEIGHT_TMT
      - fiscal_year
      - engine_id
      - month_name
      - DAY_ID
      - DISTRIBUTION_CHANNEL_CD
      - MATERIAL_GROUP_CD

  M60_LEVEL_METADATA:
    allowed_columns:
      - SBU
      - SBU_Name
      - ZONE
      - Zone_Name
      - REGION
      - Region_Name
      - SA
      - SalesArea_Name
      - PRODUCT
      - ProductName
      - UOM
      - INVOICE_DT
      - TARGET_QTY_KL
      - TARGET_QTY_TMT
      - fiscal_year
      - CUR_FISCAL_YEAR
      - ORGSBUCD
      - ORGSBUNAME
      - ORGZONECD
      - ORGZONENAME
      - ORGROCD
      - ORGRONAME
      - ORGSACD
      - ORGSANAME
      - PRODUCTCODE
      - MATERIALGROUPNAME
      - CURFISCALYEAR
      - FISCALYEAR
      - YEARMONTH
      - NETWEIGHT_UOM
      - NETWEIGHT_KG
      - NETWEIGHT_TMT
      - Total_Days_Till_PresentDay
      - Number_Of_Sundays_Till_PresentDay
      - TARGET_ROUND
      - Actual_ROUND
      - FinalSum
      - FinalActualSum
      - MaxPendingDays
      - Working_Days_Till_PresentDay_WithoutSundays
      - Rate_Per_Day_Required_MMT
      - Rate_per_day_current_MMT
      - Total_Days_in_FY
      - Pending_Days
      - month_year
      - month_name
      - fy_month
      - year_monthname
      - Target_Quantity_TMTT
      - Prediction_Value
      - Act_Tgt_Achievement
      - Zone_Region_Achievement
      - Product_Achievement
      - engine_id
```

Note: `INDUSTRY_PERFORMANCE` is documented in the PDF but is out of scope for V1.

### 6.4 Mandatory Constraints (Injected Always)

SQL fragment:

```sql
"SBU_Name" != '0'
AND "SBU_Name" NOT IN (
  'Common',
  'Mumbai Ref',
  'Renewable Energy',
  'Visakh Ref'
)
```

Implemented as:

- Hard-coded SQL fragment
- Verified by SQL validator

### 6.5 SQL Template Engine

Template example (`actual_by_month.sql.j2`):

```sql
SELECT
  month_name,
  ROUND(SUM(NETWEIGHT_TMT)::numeric, 2) AS actual_tmt
FROM MOM_DAY_LEVEL_DATA
WHERE
  SBU_Name = '{{ sbu }}'
  AND {{ mandatory_constraints }}
  AND DAY_ID BETWEEN '{{ start_date }}' AND '{{ end_date }}'
GROUP BY month_name
ORDER BY month_name;
```

Rendered using:

- Jinja2

### 6.6 SQL Validator

Rules implemented in code:

- Query starts with SELECT
- Contains mandatory constraints
- Uses only whitelisted tables
- Uses only whitelisted columns
- Has GROUP BY if aggregation used
- Has bounded date filter

Fail means reject query.

### 6.7 SQL Executor

- Uses SQLAlchemy
- Executes queries sequentially
- Captures:
  - Row count
  - Execution time
  - Result hash

### 6.8 Inference Engine (Non-LLM First)

Deterministic rules implemented in Python:

- Achievement % < 90 -> UNDERPERFORMANCE
- Two consecutive declines -> DECLINING_TREND
- Zone contributes > X% of gap -> KEY_DRIVER

Output:

```json
{
  "achievement_pct": 84.0,
  "gap_tmt": -0.21,
  "flags": ["UNDERPERFORMANCE", "DECLINING_TREND"]
}
```

### 6.9 Narrative Inference (Offline LLM)

Input to LLM:

- KPI results
- Flags
- Context only (no raw SQL, no DB)

Prompt rule:

- Explain only what the data supports
- Do not recompute values
- Do not invent causes

## 7. Reasoning Trace (Explainability)

Generated by system (not LLM):

```json
{
  "steps": [
    "Parsed intent as Actual vs Target analysis",
    "Computed monthly actual sales",
    "Computed monthly target sales",
    "Compared achievement percentages",
    "Identified zone and product drivers"
  ]
}
```

## 8. Audit Logging (Mandatory)

Each request stores:

- User question
- Plan JSON
- SQL queries
- Result hashes
- Inference flags
- Final narrative

Stored in:

- Postgres audit table, or
- JSON files

## 9. Security Rules

- LLM has no DB credentials
- SQL service isolated
- Read-only DB role
- No dynamic SQL execution
- Max row limits enforced

## 10. Deployment Order (Exact)

1. Install Postgres client libs
2. Install llama.cpp
3. Download LLM model
4. Build NL Planner service
5. Create KPI and schema registries
6. Implement SQL builder and validator
7. Implement inference rules
8. Add narrative LLM call
9. Add audit logging
10. Test with 10 fixed NL queries

## 11. Expansion Path (After v1)

- Add INDUSTRY_PERFORMANCE
- Add joins
- Add forecasting models
- Add MinIO + Trino

## 12. Final Principle

SQL produces truth. Rules interpret truth. LLM explains truth. Nothing else is allowed.
