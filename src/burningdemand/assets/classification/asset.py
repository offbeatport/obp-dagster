"""Classification asset: LLM gate (pain/would_pay/noise/confidence/lang) before embeddings."""

import asyncio
import logging
import os
import pprint
from typing import Any, List

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AutoMaterializePolicy,
    MaterializeResult,
    asset,
)
from litellm import acompletion

from burningdemand.assets.raw.model import RawItem
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


def _item_to_text(item: RawItem, max_body: int, max_comment: int) -> str:
    """Build truncated text for one item: title + body + comments."""
    return item.to_text(max_body, max_comment)


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
                api_key=os.getenv("LLM_API_KEY"),
                messages=[
                    {"role": "system", "content": build_system_prompt()},
                    {"role": "user", "content": user_prompt},
                ],
                max_tokens=cfg.llm.max_tokens,
            )
            choice = response.choices[0]
            content = choice.message.content or ""
            if not content.strip():
                finish_reason = getattr(choice, "finish_reason", None)
                usage = getattr(response, "usage", None)
                context.log.warning(
                    "LLM returned empty content. finish_reason=%s (may indicate max_tokens hit); usage=%s",
                    finish_reason,
                    usage,
                )
            arr = extract_json_array(content)
            if len(arr) != len(url_hashes):
                context.log.warning(
                    "LLM returned %s items, expected %s (truncation?); persisting partial results",
                    len(arr),
                    len(url_hashes),
                )
            # Pair by min length to avoid index errors; missing items are skipped
            n = min(len(arr), len(url_hashes))
            return [
                _label_to_row(
                    PainClassification.model_validate(arr[i]), url_hashes[i], date
                )
                for i in range(n)
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

    items: List[RawItem] = db.query_df(
        """
            SELECT url_hash, title, body, comments_list
            FROM bronze.raw_items
            WHERE collected_at = ?
                AND comments_count > 0
                AND reactions_count > 0
                AND product_stars > 0
        """,
        [date],
        model=RawItem,
    )

    if len(items) == 0:
        context.log.info("No items to classify for %s", date)
        return MaterializeResult(metadata={"classified": 0})

    db.execute(
        "DELETE FROM silver.classifications WHERE classification_date = ?", [date]
    )

    total_batches = (len(items) + cfg.batch_size - 1) // cfg.batch_size
    max_concurrent = min(cfg.max_concurrent_batches, total_batches)
    context.log.info(
        "Preparing to classify %s items in %s batches (batch_size=%s, max_concurrent=%s)",
        len(items),
        total_batches,
        cfg.batch_size,
        max_concurrent,
    )

    sem = asyncio.Semaphore(max_concurrent)

    async def run_batch(batch_num: int, batch: List[RawItem]) -> int:
        async with sem:
            context.log.info(
                "Batch %s of %s: classifying %s items",
                batch_num,
                total_batches,
                len(batch),
            )
            url_hashes = [item.url_hash for item in batch]
            items_text = [
                _item_to_text(item, cfg.max_body_length, cfg.max_comment_length)
                for item in batch
            ]
            rows = await _classify_batch(context, items_text, url_hashes, date)
            if rows:
                db.upsert_df(
                    "silver", "classifications", pd.DataFrame(rows), UPSERT_COLUMNS
                )
            return len(rows)

    batches = [
        (i // cfg.batch_size + 1, items[i : i + cfg.batch_size])
        for i in range(0, len(items), cfg.batch_size)
    ]
    counts = await asyncio.gather(
        *[run_batch(batch_num, batch) for batch_num, batch in batches]
    )
    total = sum(counts)

    return MaterializeResult(
        metadata={
            "classified": total,
            "batch_size": cfg.batch_size,
            "pain_threshold": cfg.pain_threshold,
        }
    )
