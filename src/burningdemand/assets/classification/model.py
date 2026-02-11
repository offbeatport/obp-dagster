"""LLM output schema for pain classification."""

import json
import re

from pydantic import BaseModel, Field


class PainClassification(BaseModel):
    """Per-item pain classifier output."""
    pain: float = Field(ge=0.0, le=1.0)
    would_pay: float = Field(ge=0.0, le=1.0)
    noise: float = Field(ge=0.0, le=1.0)
    confidence: float = Field(ge=0.0, le=1.0, default=0.5)
    lang: str = Field(default="en", description="ISO 639-1 language code, e.g. en, es")


def extract_json_array(text: str) -> list:
    """Extract first JSON array from model output."""
    m = re.search(r"\[.*\]", text, flags=re.S)
    if not m:
        raise ValueError("No JSON array found in model output")
    return json.loads(m.group(0))
