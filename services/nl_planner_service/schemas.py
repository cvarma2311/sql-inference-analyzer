from pydantic import BaseModel, Field


class PlanRequest(BaseModel):
    question: str = Field(..., min_length=3)


class PlanResponse(BaseModel):
    objective: str
    time_range: str
    level: str
    drilldown_path: list[str]
    driver_levels: list[str]
    filters: dict[str, str]
    queries: list[str]
    fiscal_year: str | None = None
    sbu: str | None = None
    breakdowns: list[str] | None = None
