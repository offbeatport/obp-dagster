"""LLM output schema for pain classification."""

import json
import logging
import re

from pydantic import BaseModel, Field

log = logging.getLogger(__name__)


class PainClassification(BaseModel):
    """Per-item pain classifier output."""

    pain: float = Field(ge=0.0, le=1.0)
    would_pay: float = Field(ge=0.0, le=1.0)
    noise: float = Field(ge=0.0, le=1.0)
    confidence: float = Field(ge=0.0, le=1.0, default=0.5)
    lang: str = Field(default="en", description="ISO 639-1 language code, e.g. en, es")


def extract_json_array(text: str) -> list:
    """Extract array of JSON objects from model output. Handles both JSON array and NDJSON (one object per line)."""
    m = re.search(r"\[.*\]", text, flags=re.S)
    if m:
        return json.loads(m.group(0))

    # Fallback: NDJSON (one JSON object per line)
    result = []
    for line in (text or "").strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        obj_match = re.search(r"\{.*\}", line, flags=re.S)
        if obj_match:
            try:
                result.append(json.loads(obj_match.group(0)))
            except json.JSONDecodeError:
                pass
    if result:
        log.info(
            "Parsed %s objects from NDJSON format (model returned objects per line instead of array)",
            len(result),
        )
        return result

    content = text or ""
    max_log = 1500
    preview = content[:max_log] + ("..." if len(content) > max_log else "")
    log.warning(
        "No JSON array or NDJSON in model output. content_len=%s, response_preview=%r",
        len(content),
        preview,
    )
    raise ValueError("No JSON array found in model output")
