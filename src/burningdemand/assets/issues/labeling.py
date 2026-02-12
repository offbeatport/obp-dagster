"""LLM labeling for issues: call LLM to label groups with retries."""

import asyncio
import logging
import os
from typing import List

from litellm import acompletion

from burningdemand.utils.config import config
from .model import IssueLabel, extract_first_json_obj
from .prompt import build_system_prompt, build_user_prompt

log = logging.getLogger(__name__)

LLM_RETRY_ATTEMPTS = 3
LLM_RETRY_BACKOFF_SEC = [1.0, 2.0, 4.0]


async def label_group_with_llm(
    gid: int,
    size: int,
    authority_score: float,
    titles: List[str],
    snippets: str,
    date: str,
    sem: asyncio.Semaphore,
    results: List[dict],
    failed: List[dict],
    errors: List[str],
) -> None:
    """Label one group via LLM. Retries on transient errors with backoff."""
    async with sem:
        prompt = build_user_prompt(titles, size, snippets)
        label_data = None
        last_error = None
        model = config.issues.llm.model

        for attempt in range(LLM_RETRY_ATTEMPTS):
            try:
                response = await acompletion(
                    model=model,
                    base_url=config.issues.llm.base_url,
                    api_key=os.getenv("LLM_API_KEY"),
                    messages=[
                        {
                            "role": "system",
                            "content": build_system_prompt(),
                        },
                        {"role": "user", "content": prompt},
                    ],
                    max_tokens=config.issues.llm.max_tokens,
                )
                choice = response.choices[0]
                raw = choice.message.content or ""
                if not raw.strip():
                    finish_reason = getattr(choice, "finish_reason", None)
                    usage = getattr(response, "usage", None)
                    log.warning(
                        "group_id=%s: LLM returned empty content. finish_reason=%s (may indicate max_tokens hit); usage=%s",
                        gid,
                        finish_reason,
                        usage,
                    )
                data = extract_first_json_obj(raw)
                label = IssueLabel.model_validate(data)
                label_data = {
                    "canonical_title": label.canonical_title,
                    "category": ",".join(label.category) if label.category else "other",
                    "desc_problem": label.desc_problem or "",
                    "desc_current_solutions": label.desc_current_solutions or "",
                    "desc_impact": label.desc_impact or "",
                    "desc_details": label.desc_details or "",
                    "would_pay_signal": bool(label.would_pay_signal),
                    "impact_level": label.impact_level,
                }
                break
            except Exception as e:
                last_error = e
                if attempt < LLM_RETRY_ATTEMPTS - 1:
                    delay = LLM_RETRY_BACKOFF_SEC[attempt]
                    log.warning(
                        "group_id=%s attempt %s/%s: %s; retrying in %ss",
                        gid,
                        attempt + 1,
                        LLM_RETRY_ATTEMPTS,
                        e,
                        delay,
                    )
                    await asyncio.sleep(delay)
                else:
                    log.warning("group_id=%s: %s", gid, e, exc_info=True)

        if label_data:
            results.append(
                {
                    "group_date": date,
                    "group_id": int(gid),
                    "canonical_title": label_data["canonical_title"],
                    "category": label_data["category"],
                    "desc_problem": label_data["desc_problem"],
                    "desc_current_solutions": label_data["desc_current_solutions"],
                    "desc_impact": label_data["desc_impact"],
                    "desc_details": label_data["desc_details"],
                    "would_pay_signal": label_data["would_pay_signal"],
                    "impact_level": label_data["impact_level"],
                    "group_size": int(size),
                    "authority_score": float(authority_score),
                    "label_failed": False,
                }
            )
        else:
            failed.append(
                {
                    "group_date": date,
                    "group_id": int(gid),
                    "label_failed": True,
                }
            )
            errors.append(f"group_id={gid}: {last_error}")
