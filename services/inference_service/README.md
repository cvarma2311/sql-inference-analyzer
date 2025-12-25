# Inference Service (Deterministic)

## Goal
Compute deterministic KPIs and flags without LLM involvement.

## Local Usage

```bash
python -c "from services.inference_service.rules import run_inference; print(run_inference(84.0, 100.0, [100, 95, 90], 35.0))"
```

## Rules

- Achievement % < 90 -> UNDERPERFORMANCE
- Two consecutive declines -> DECLINING_TREND
- Zone contributes > 30% of gap -> KEY_DRIVER

## Inputs

- `actual_tmt` (float)
- `target_tmt` (float)
- `trend_values` (list of floats, optional)
- `contribution_pct` (float, optional)

## Outputs

- `achievement_pct`
- `gap_tmt`
- `flags`
