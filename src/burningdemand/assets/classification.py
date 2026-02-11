"""Classification asset: LLM gate (pain/would_pay/noise) + language detection before clustering."""

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
from burningdemand.utils.llm_schema import PainClassification, extract_json_array

log = logging.getLogger(__name__)

LLM_RETRY_ATTEMPTS = 3
LLM_RETRY_BACKOFF_SEC = [1.0, 2.0, 4.0]


def _truncate_middle(text: str, max_len: int) -> str:
    """Truncate to max_len, keeping first and last parts with '...' in the middle."""
    if not text or len(text) <= max_len:
        return text or ""
    half = (max_len - 3) // 2
    return text[:half] + "..." + text[-half:]


def _comments_to_text(comments_raw: Any, max_comment_len: int) -> str:
    """Parse comments_list JSON and format as text, truncating each comment."""
    if comments_raw is None:
        return ""
    try:
        if isinstance(comments_raw, str):
            comments = json.loads(comments_raw)
        else:
            comments = comments_raw
    except (json.JSONDecodeError, TypeError):
        return ""
    if not isinstance(comments, list):
        return ""
    parts: List[str] = []
    for c in comments:
        if isinstance(c, dict):
            body = c.get("body") or ""
        else:
            body = str(c)
        if body:
            parts.append(_truncate_middle(body, max_comment_len))
    return "\n".join(parts) if parts else ""


def _item_to_text(row: Any, max_body: int, max_comment: int) -> str:
    """Build truncated text for one item: title + body + comments."""
    title = str(row.get("title") or "")
    body = _truncate_middle(str(row.get("body") or ""), max_body)
    comments = _comments_to_text(row.get("comments_list"), max_comment)
    sections = [f"Title: {title}", f"Body: {body}"]
    if comments:
        sections.append(f"Comments:\n{comments}")
    return "\n\n".join(sections)


async def _classify_batch(
    context: AssetExecutionContext,
    items_text: List[str],
    url_hashes: List[str],
    date: str,
) -> List[dict]:
    """Call LLM to classify a batch; return list of {url_hash, pain_prob, would_pay_prob, noise_prob, confidence, language}."""
    cfg = config.classification
    prompt = config.build_classification_prompt(items_text)
    system = config.prompts.get(
        "classification_system", config.build_system_prompt_lite()
    )

    last_error = None
    for attempt in range(LLM_RETRY_ATTEMPTS):
        try:
            response = await acompletion(
                model=cfg.llm.model,
                base_url=cfg.llm.base_url,
                api_key=cfg.llm.api_key,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=cfg.llm.max_tokens,
            )
            raw = response.choices[0].message.content
            arr = extract_json_array(raw)
            if len(arr) != len(url_hashes):
                raise ValueError(
                    f"LLM returned {len(arr)} items, expected {len(url_hashes)}"
                )
            results = []
            for i, obj in enumerate(arr):
                label = PainClassification.model_validate(obj)
                results.append(
                    {
                        "url_hash": url_hashes[i],
                        "classification_date": date,
                        "pain_prob": float(label.pain),
                        "would_pay_prob": float(label.would_pay),
                        "noise_prob": float(label.noise),
                        "confidence": float(label.confidence),
                        "language": (label.lang or "en")[:10],
                    }
                )
            return results
        except Exception as e:
            last_error = e
            if attempt < LLM_RETRY_ATTEMPTS - 1:
                delay = LLM_RETRY_BACKOFF_SEC[attempt]
                context.log.warning(
                    "classification batch attempt %s/%s: %s; retrying in %ss",
                    attempt + 1,
                    LLM_RETRY_ATTEMPTS,
                    e,
                    delay,
                )
                await asyncio.sleep(delay)
            else:
                raise last_error
    raise last_error


@asset(
    partitions_def=daily_partitions,
    group_name="silver",
    deps=[
        "raw_gh_issues",
        "raw_gh_discussions",
        "raw_gh_pull_requests",
    ],
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    description="Classify raw items via LLM (pain/would_pay/noise/confidence/lang). Gate before clustering: only items above pain_threshold proceed to embeddings.",
)
async def classifications(
    context: AssetExecutionContext,
    db: DuckDBResource,
) -> MaterializeResult:
    date = context.partition_key
    cfg = config.classification

    items = db.query_df(
        """
        SELECT url_hash, title, body, comments_list
        FROM bronze.raw_items
        WHERE CAST(created_at AS DATE) = ?
        """,
        [date],
    )

    if len(items) == 0:
        context.log.info("No items to classify for %s", date)
        return MaterializeResult(metadata={"classified": 0})

    # Clear existing classifications for this date
    db.execute(
        "DELETE FROM silver.classifications WHERE classification_date = ?",
        [date],
    )

    batch_size = cfg.batch_size
    total_batches = (len(items) + batch_size - 1) // batch_size
    total_classified = 0

    for i in range(0, len(items), batch_size):
        batch_num = (i // batch_size) + 1
        batch = items.iloc[i : i + batch_size]
        url_hashes = batch["url_hash"].tolist()
        items_text = [
            _item_to_text(row, cfg.max_body_length, cfg.max_comment_length)
            for _, row in batch.iterrows()
        ]

        context.log.info(
            "Batch %s/%s: classifying %s items",
            batch_num,
            total_batches,
            len(batch),
        )

        classifications = await _classify_batch(
            context, items_text, url_hashes, date
        )
        df = pd.DataFrame(classifications)
        db.upsert_df(
            "silver",
            "classifications",
            df,
            [
                "url_hash",
                "classification_date",
                "pain_prob",
                "would_pay_prob",
                "noise_prob",
                "confidence",
                "language",
            ],
        )
        total_classified += len(classifications)

    return MaterializeResult(
        metadata={
            "classified": int(total_classified),
            "batch_size": int(batch_size),
            "pain_threshold": float(cfg.pain_threshold),
        }
    )
