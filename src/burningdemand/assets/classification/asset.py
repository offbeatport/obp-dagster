"""Classification asset: LLM gate (pain/would_pay/noise/confidence/lang) before embeddings."""

import asyncio
import json
import logging
from typing import Any, List

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AutoMaterializePolicy,
    MaterializeResult,
    asset,
)
from litellm import acompletion

from burningdemand.partitions import daily_partitions
from burningdemand.resources.duckdb_resource import DuckDBResource
from burningdemand.utils.config import config
from .model import PainClassification, extract_json_array

from .prompt import build_system_prompt, build_user_prompt

log = logging.getLogger(__name__)

LLM_RETRY_ATTEMPTS = 3
LLM_RETRY_BACKOFF = [1.0, 2.0, 4.0]
UPSERT_COLUMNS = [
    "url_hash",
    "classification_date",
    "pain_prob",
    "would_pay_prob",
    "noise_prob",
    "confidence",
    "language",
]


def _truncate_middle(text: str, max_len: int) -> str:
    """Truncate to max_len, keeping first and last parts."""
    if not text or len(text) <= max_len:
        return text or ""
    half = (max_len - 3) // 2
    return text[:half] + "..." + text[-half:]


def _comments_to_text(comments_raw: Any, max_len: int) -> str:
    """Parse comments_list and format as text, truncating each comment."""
    if comments_raw is None:
        return ""
    try:
        comments = (
            json.loads(comments_raw) if isinstance(comments_raw, str) else comments_raw
        )
    except (json.JSONDecodeError, TypeError):
        return ""
    if not isinstance(comments, list):
        return ""
    parts = [
        _truncate_middle(
            (c.get("body") or "") if isinstance(c, dict) else str(c), max_len
        )
        for c in comments
    ]
    return "\n".join(p for p in parts if p)


def _item_to_text(row: Any, max_body: int, max_comment: int) -> str:
    """Build truncated text for one item: title + body + comments."""
    title = str(row.get("title") or "")
    body = _truncate_middle(str(row.get("body") or ""), max_body)
    comments = _comments_to_text(row.get("comments_list"), max_comment)
    sections = [f"Title: {title}", f"Body: {body}"]
    if comments:
        sections.append(f"Comments:\n{comments}")
    return "\n\n".join(sections)


def _label_to_row(label: PainClassification, url_hash: str, date: str) -> dict:
    """Convert LLM label to DB row."""
    return {
        "url_hash": url_hash,
        "classification_date": date,
        "pain_prob": float(label.pain),
        "would_pay_prob": float(label.would_pay),
        "noise_prob": float(label.noise),
        "confidence": float(label.confidence),
        "language": (label.lang or "en")[:10],
    }


async def _classify_batch(
    context: AssetExecutionContext,
    items_text: List[str],
    url_hashes: List[str],
    date: str,
) -> List[dict]:
    """Call LLM to classify a batch. Retries on failure."""
    cfg = config.classification
    user_prompt = build_user_prompt(items_text)

    for attempt in range(LLM_RETRY_ATTEMPTS):
        try:
            response = await acompletion(
                model=cfg.llm.model,
                base_url=cfg.llm.base_url,
                api_key=cfg.llm.api_key,
                messages=[
                    {"role": "system", "content": build_system_prompt()},
                    {"role": "user", "content": user_prompt},
                ],
                max_tokens=cfg.llm.max_tokens,
            )
            arr = extract_json_array(response.choices[0].message.content)
            if len(arr) != len(url_hashes):
                raise ValueError(
                    f"LLM returned {len(arr)} items, expected {len(url_hashes)}"
                )
            return [
                _label_to_row(
                    PainClassification.model_validate(obj), url_hashes[i], date
                )
                for i, obj in enumerate(arr)
            ]
        except Exception as e:
            if attempt < LLM_RETRY_ATTEMPTS - 1:
                delay = LLM_RETRY_BACKOFF[attempt]
                context.log.warning(
                    "classification attempt %s/%s: %s; retrying in %ss",
                    attempt + 1,
                    LLM_RETRY_ATTEMPTS,
                    e,
                    delay,
                )
                await asyncio.sleep(delay)
            else:
                raise


@asset(
    partitions_def=daily_partitions,
    group_name="silver",
    deps=["raw_gh_issues", "raw_gh_discussions", "raw_gh_pull_requests"],
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    description="Classify raw items via LLM (pain/would_pay/noise/confidence/lang). Gate before embeddings: only items above pain_threshold proceed.",
)
async def classifications(
    context: AssetExecutionContext,
    db: DuckDBResource,
) -> MaterializeResult:
    date = context.partition_key
    cfg = config.classification

    items = db.query_df(
        "SELECT url_hash, title, body, comments_list FROM bronze.raw_items WHERE CAST(created_at AS DATE) = ?",
        [date],
    )
    if len(items) == 0:
        context.log.info("No items to classify for %s", date)
        return MaterializeResult(metadata={"classified": 0})

    db.execute(
        "DELETE FROM silver.classifications WHERE classification_date = ?", [date]
    )

    total = 0
    for i in range(0, len(items), cfg.batch_size):
        batch = items.iloc[i : i + cfg.batch_size]
        url_hashes = batch["url_hash"].tolist()
        items_text = [
            _item_to_text(row, cfg.max_body_length, cfg.max_comment_length)
            for _, row in batch.iterrows()
        ]
        context.log.info("Batch: classifying %s items", len(batch))

        rows = await _classify_batch(context, items_text, url_hashes, date)
        db.upsert_df("silver", "classifications", pd.DataFrame(rows), UPSERT_COLUMNS)
        total += len(rows)

    return MaterializeResult(
        metadata={
            "classified": total,
            "batch_size": cfg.batch_size,
            "pain_threshold": cfg.pain_threshold,
        }
    )
