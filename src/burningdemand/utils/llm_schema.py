# burningdemand_dagster/utils/llm_schema.py
import json
import re
from typing import List, Literal

from pydantic import BaseModel, Field, field_validator

CATEGORIES = (
    "ai",
    "finance",
    "data",
    "compliance",
    "security",
    "payments",
    "devtools",
    "infrastructure",
    "other",
)


class IssueLabel(BaseModel):
    canonical_title: str = Field()
    category: List[str] = Field(
        description="One or more of: " + ", ".join(CATEGORIES),
    )
    desc_problem: str = Field(
        description="What breaks or fails; blocked workflow; immediate pain.",
    )
    desc_current_solutions: str = Field(
        default="",
        description="What people try now; why current tools/workarounds fail.",
    )
    desc_impact: str = Field(
        default="",
        description="Who is affected; when it hurts; downstream consequences.",
    )
    desc_details: str = Field(
        default="",
        description="Technical constraints, scale, integration if relevant.",
    )
    would_pay_signal: bool
    impact_level: Literal["low", "medium", "high"]

    @field_validator("category", mode="before")
    @classmethod
    def validate_category(cls, v: object) -> List[str]:
        if isinstance(v, str):
            v = [v]
        if not isinstance(v, list):
            raise ValueError("category must be a list of strings")
        out = []
        for x in v:
            s = str(x).strip().lower()
            if s and s in CATEGORIES:
                out.append(s)
        return out if out else ["other"]


class PainClassification(BaseModel):
    """Per-item pain classifier output."""
    pain: float = Field(ge=0.0, le=1.0)
    would_pay: float = Field(ge=0.0, le=1.0)
    noise: float = Field(ge=0.0, le=1.0)


def extract_first_json_obj(text: str) -> dict:
    m = re.search(r"\{.*\}", text, flags=re.S)
    if not m:
        raise ValueError("No JSON object found in model output")
    return json.loads(m.group(0))


def extract_json_array(text: str) -> list:
    """Extract first JSON array from model output."""
    m = re.search(r"\[.*\]", text, flags=re.S)
    if not m:
        raise ValueError("No JSON array found in model output")
    return json.loads(m.group(0))
