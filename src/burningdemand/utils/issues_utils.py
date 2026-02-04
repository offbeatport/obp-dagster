"""
Helpers for the issues asset: cluster queries, LLM labeling, and DB writes.
"""

import asyncio
import logging
from pprint import pprint
from typing import Dict, List, Tuple

log = logging.getLogger(__name__)

import numpy as np
import pandas as pd
from litellm import acompletion

from burningdemand.resources.duckdb_resource import DuckDBResource
from burningdemand.utils.cluster_representatives import get_cluster_representatives
from burningdemand.config import (
    LLM_API_KEY,
    LLM_BASE_URL,
    LLM_MAX_TOKENS,
    LLM_MODEL,
    MAX_BODY_LENGTH_FOR_SNIPPET,
    MAX_REPRESENTATIVES_FOR_LABELING,
    MAX_SNIPPETS_FOR_LABELING,
)
from burningdemand.utils.llm_schema import IssueLabel, extract_first_json_obj
from burningdemand.utils.prompts import build_label_prompt, build_system_prompt


def clear_issues_data_for_date(db: DuckDBResource, date: str) -> None:
    """Remove existing gold.issues and gold.issue_evidence for this partition (enables full recompute on re-materialize)."""
    db.execute("DELETE FROM gold.issue_evidence WHERE cluster_date = ?", [date])
    db.execute("DELETE FROM gold.issues WHERE cluster_date = ?", [date])


def get_clusters_for_date(db: DuckDBResource, date: str) -> pd.DataFrame:
    """All clusters for this date, ranked by authority_score (descending). Used when recomputing labels for the partition."""
    return db.query_df(
        """
        SELECT sc.cluster_id, sc.cluster_size, sc.authority_score
        FROM silver.clusters sc
        WHERE sc.cluster_date = ?
        ORDER BY sc.authority_score DESC, sc.cluster_size DESC
        LIMIT 1
        """,
        [date],
    )


def prepare_clusters(
    db: DuckDBResource,
    date: str,
    cluster_ids: List[int],
) -> Tuple[Dict[int, List[str]], Dict[int, str], Dict[int, str]]:
    """Load cluster items from DB and compute representative titles + snippets per cluster."""
    titles_by_cluster: Dict[int, List[str]] = {}
    snippets_by_cluster: Dict[int, str] = {}

    for cid in cluster_ids:
        cluster_data = db.query_df(
            """
            SELECT
                e.url_hash,
                e.embedding,
                b.title,
                b.body,
                b.source
            FROM silver.embeddings e
            JOIN bronze.raw_items b ON e.url_hash = b.url_hash
            JOIN silver.cluster_assignments ca ON e.url_hash = ca.url_hash
            WHERE ca.cluster_date = ?
              AND ca.cluster_id = ?
            """,
            [date, cid],
        )

        if len(cluster_data) == 0:
            titles_by_cluster[cid] = []
            snippets_by_cluster[cid] = ""
            continue

        embeddings_array = np.array(
            cluster_data["embedding"].tolist(), dtype=np.float32
        )
        cluster_items = pd.DataFrame(
            {
                "title": cluster_data["title"],
                "body": cluster_data["body"],
                "source": cluster_data["source"],
            }
        )
        titles, snippets = get_cluster_representatives(
            cluster_items,
            embeddings_array,
            max_representatives_count=MAX_REPRESENTATIVES_FOR_LABELING,
            max_snippets_count=MAX_SNIPPETS_FOR_LABELING,
            max_body_length=MAX_BODY_LENGTH_FOR_SNIPPET,
        )
        titles_by_cluster[cid] = titles
        snippets_by_cluster[cid] = snippets

    return titles_by_cluster, snippets_by_cluster


async def label_cluster_with_llm(
    cid: int,
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
    """Label one cluster via LLM. Model from env LLM_MODEL (default: ollama/gemma3:4b)."""
    async with sem:

        prompt = build_label_prompt(titles, size, snippets)
        label_data = None
        last_error = None
        model = LLM_MODEL

        try:
            response = await acompletion(
                model=model,
                base_url=LLM_BASE_URL,
                api_key=LLM_API_KEY,
                messages=[
                    {"role": "system", "content": build_system_prompt()},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=LLM_MAX_TOKENS,
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
        except Exception as e:
            last_error = e
            log.warning("cluster_id=%s: %s", cid, e, exc_info=True)

        if label_data:
            results.append(
                {
                    "cluster_date": date,
                    "cluster_id": int(cid),
                    "canonical_title": label_data["canonical_title"],
                    "category": label_data["category"],
                    "desc_problem": label_data["desc_problem"],
                    "desc_current_solutions": label_data["desc_current_solutions"],
                    "desc_impact": label_data["desc_impact"],
                    "desc_details": label_data["desc_details"],
                    "would_pay_signal": label_data["would_pay_signal"],
                    "impact_level": label_data["impact_level"],
                    "cluster_size": int(size),
                    "authority_score": float(authority_score),
                    "label_failed": False,
                }
            )
        else:
            failed.append(
                {
                    "cluster_date": date,
                    "cluster_id": int(cid),
                    "label_failed": True,
                }
            )
            errors.append(f"cluster_id={cid}: {last_error}")


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
                "cluster_date",
                "cluster_id",
                "canonical_title",
                "category",
                "desc_problem",
                "desc_current_solutions",
                "desc_impact",
                "desc_details",
                "would_pay_signal",
                "impact_level",
                "cluster_size",
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
                "cluster_date",
                "cluster_id",
                "label_failed",
            ],
        )

    # Backfill evidence for all successfully labeled issues for this date
    # (items that belong to each cluster), so gold.issue_evidence stays in sync.
    if results or failed:
        db.execute(
            """
            INSERT INTO gold.issue_evidence (cluster_date, cluster_id, url_hash, source, url, title, body, posted_at)
            SELECT ca.cluster_date,
                   ca.cluster_id,
                   ca.url_hash,
                   b.source,
                   b.url,
                   b.title,
                   b.body,
                   b.created_at
            FROM silver.cluster_assignments ca
            JOIN bronze.raw_items b
              ON ca.url_hash = b.url_hash
            WHERE ca.cluster_date = ?
              AND EXISTS (
                  SELECT 1 FROM gold.issues gi
                  WHERE gi.cluster_date = ca.cluster_date
                    AND gi.cluster_id = ca.cluster_id
                    AND gi.label_failed = FALSE
              )
            ON CONFLICT DO NOTHING
            """,
            [date],
        )
