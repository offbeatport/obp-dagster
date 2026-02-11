"""
Helpers for the issues asset: group queries, LLM labeling, and DB writes.
"""

import asyncio
import logging
from typing import Dict, List, Tuple

log = logging.getLogger(__name__)

import numpy as np
import pandas as pd
from litellm import acompletion

from burningdemand.resources.duckdb_resource import DuckDBResource
from burningdemand.utils.cluster_representatives import get_cluster_representatives
from burningdemand.utils.config import config
from burningdemand.utils.llm_schema import IssueLabel, extract_first_json_obj


def clear_issues_data_for_date(db: DuckDBResource, date: str) -> None:
    """Remove existing gold.issues and gold.issue_evidence for this partition (enables full recompute on re-materialize)."""
    db.execute("DELETE FROM gold.issue_evidence WHERE group_date = ?", [date])
    db.execute("DELETE FROM gold.issues WHERE group_date = ?", [date])


def get_groups_for_date(db: DuckDBResource, date: str) -> pd.DataFrame:
    """All groups for this date, ranked by authority_score (descending). Used when recomputing labels for the partition."""
    return db.query_df(
        """
        SELECT sg.group_id, sg.group_size, sg.authority_score
        FROM silver.groups sg
        WHERE sg.group_date = ?
        ORDER BY sg.authority_score DESC, sg.group_size DESC
        """,
        [date],
    )


def prepare_groups(
    db: DuckDBResource,
    date: str,
    group_ids: List[int],
) -> Tuple[Dict[int, List[str]], Dict[int, str]]:
    """Load group items from DB and compute representative titles + snippets per group."""
    titles_by_group: Dict[int, List[str]] = {}
    snippets_by_group: Dict[int, str] = {}

    for gid in group_ids:
        group_data = db.query_df(
            """
            SELECT
                e.url_hash,
                e.embedding,
                b.title,
                b.body,
                b.source
            FROM silver.embeddings e
            JOIN bronze.raw_items b ON e.url_hash = b.url_hash
            JOIN silver.group_members gm ON e.url_hash = gm.url_hash
            WHERE gm.group_date = ?
              AND gm.group_id = ?
            """,
            [date, gid],
        )

        if len(group_data) == 0:
            titles_by_group[gid] = []
            snippets_by_group[gid] = ""
            continue

        embeddings_array = np.array(
            group_data["embedding"].tolist(), dtype=np.float32
        )
        group_items = pd.DataFrame(
            {
                "title": group_data["title"],
                "body": group_data["body"],
                "source": group_data["source"],
            }
        )
        titles, snippets = get_cluster_representatives(
            group_items,
            embeddings_array,
            max_representatives_count=config.issues.labeling.max_representatives_for_labeling,
            max_snippets_count=config.issues.labeling.max_snippets_for_labeling,
            max_body_length=config.issues.labeling.max_body_length_for_snippet,
        )
        titles_by_group[gid] = titles
        snippets_by_group[gid] = snippets

    return titles_by_group, snippets_by_group


# Retry for transient LLM API errors
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
        prompt = config.build_label_prompt(titles, size, snippets)
        label_data = None
        last_error = None
        model = config.issues.llm.model

        for attempt in range(LLM_RETRY_ATTEMPTS):
            try:
                response = await acompletion(
                    model=model,
                    base_url=config.issues.llm.base_url,
                    api_key=config.issues.llm.api_key,
                    messages=[
                        {"role": "system", "content": config.build_system_prompt()},
                        {"role": "user", "content": prompt},
                    ],
                    max_tokens=config.issues.llm.max_tokens,
                )
                raw = response.choices[0].message.content
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


def save_results(
    db: DuckDBResource,
    results: List[dict],
    failed: List[dict],
    date: str,
) -> None:
    """Persist labeled issues, failed rows, and issue_evidence to gold."""
    if results:
        df = pd.DataFrame(results)
        db.upsert_df(
            "gold",
            "issues",
            df,
            [
                "group_date",
                "group_id",
                "canonical_title",
                "category",
                "desc_problem",
                "desc_current_solutions",
                "desc_impact",
                "desc_details",
                "would_pay_signal",
                "impact_level",
                "group_size",
                "authority_score",
                "label_failed",
            ],
        )

    if failed:
        failed_df = pd.DataFrame(failed)
        db.upsert_df(
            "gold",
            "issues",
            failed_df,
            [
                "group_date",
                "group_id",
                "label_failed",
            ],
        )

    # Backfill evidence for all successfully labeled issues for this date
    # (items that belong to each group), so gold.issue_evidence stays in sync.
    if results or failed:
        db.execute(
            """
            INSERT INTO gold.issue_evidence (group_date, group_id, url_hash, source, url, title, body, posted_at)
            SELECT gm.group_date,
                   gm.group_id,
                   gm.url_hash,
                   b.source,
                   b.url,
                   b.title,
                   b.body,
                   b.created_at
            FROM silver.group_members gm
            JOIN bronze.raw_items b
              ON gm.url_hash = b.url_hash
            WHERE gm.group_date = ?
              AND EXISTS (
                  SELECT 1 FROM gold.issues gi
                  WHERE gi.group_date = gm.group_date
                    AND gi.group_id = gm.group_id
                    AND gi.label_failed = FALSE
              )
            ON CONFLICT DO NOTHING
            """,
            [date],
        )
