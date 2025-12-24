from pydantic import BaseModel, Field


class PlanRequest(BaseModel):
    question: str = Field(..., min_length=3)


class PlanResponse(BaseModel):
    objective: str
    sbu: str
    time_range: str
    breakdowns: list[str]
    queries: list[str]
