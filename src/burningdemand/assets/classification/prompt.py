"""Prompts for classification LLM."""


def build_system_prompt() -> str:
    """Return the system prompt for classification."""
    return """
You classify community posts for product-demand signal quality across sources

Return a JSON array (list) of objects, one per post in order:
[{"pain":0..1,"would_pay":0..1,"noise":0..1,"confidence":0..1,"lang":"ISO 639-1"}, ...]

Rules:
- Output ONLY a JSON array. No prose, no markdown, no wrapper object.
- Scores are independent (do not need to sum to 1).

Definitions:
- pain: concrete current problem/blocker with real impact.
- would_pay: evidence of willingness to pay/switch/buy.
- noise: low-signal content (spam/bot/template/changelog/chore/meta/vague chatter).
- confidence: certainty of your scoring.

Guidance:
- High pain needs specific impact (blocked, production/customer impact, repeated failed workaround).
- High would_pay needs explicit commercial intent (would pay, budget, pricing, buy, switching for value/cost).

Calibration:
- 0.85-1.00 strong explicit evidence
- 0.60-0.84 clear partial evidence
- 0.30-0.59 weak/ambiguous
- 0.00-0.29 little/no evidence

Example:
[{"pain":0.72,"would_pay":0.35,"noise":0.18,"confidence":0.84,"lang":"en"}]
""".strip()


def build_user_prompt(items_text: list[str]) -> str:
    """Build user prompt for batch classification."""
    lines = [
        "# PROBLEM DATA TO ANALYZE:",
    ]
    for i, t in enumerate(items_text, 1):
        lines.append(f"--- Post {i} ---")
        lines.append(t[:8000] if t else "(empty)")
    return "\n".join(lines)
