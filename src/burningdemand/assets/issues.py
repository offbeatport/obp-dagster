# burningdemand_dagster/assets/issues.py
import asyncio
import hashlib
import json
import os
from typing import Dict, List, Optional, Tuple
import pandas as pd
from litellm import acompletion

from dagster import (
    AssetExecutionContext,
    AssetKey,
    MaterializeResult,
    MetadataValue,
    asset,
)

from burningdemand.partitions import daily_partitions
from burningdemand.resources.llm_resource import LLMResource
from burningdemand.resources.duckdb_resource import DuckDBResource
from burningdemand.resources.embedding_resource import EmbeddingResource
from burningdemand.utils.llm_schema import IssueLabel, extract_first_json_obj


def get_unlabeled_clusters(db: DuckDBResource, date: str) -> pd.DataFrame:
    """Get clusters that haven't been labeled yet, ranked by authority_score (descending)."""
    return db.query_df(
        """
        SELECT sc.cluster_id, sc.cluster_size, sc.authority_score
        FROM silver.clusters sc
        WHERE sc.cluster_date = ?
          AND NOT EXISTS (
              SELECT 1 FROM gold.issues gi
              WHERE gi.cluster_date = sc.cluster_date
                AND gi.cluster_id = sc.cluster_id
          )
        ORDER BY sc.authority_score DESC, sc.cluster_size DESC
        """,
        [date],
    )


def normalize_title_for_fingerprint(title: str) -> str:
    """Normalize title for fingerprint computation."""
    return title.lower().strip()


def compute_cluster_fingerprint(titles: List[str], k: int = 5) -> str:
    """Compute cluster fingerprint from normalized representative titles."""
    normalized = [normalize_title_for_fingerprint(t) for t in titles if t]
    sorted_titles = sorted(normalized)[:k]
    fingerprint_str = "|".join(sorted_titles)
    return hashlib.sha256(fingerprint_str.encode("utf-8")).hexdigest()


def get_existing_label_by_fingerprint(
    db: DuckDBResource, fingerprint: str
) -> Optional[dict]:
    """Check if a label exists for this fingerprint and return it."""
    result = db.query_df(
        """
        SELECT 
            canonical_title, category, description,
            would_pay_signal, impact_level, cluster_size
        FROM gold.issues
        WHERE cluster_fingerprint = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        [fingerprint],
    )
    if len(result) > 0:
        row = result.iloc[0]
        return {
            "canonical_title": str(row["canonical_title"]),
            "category": str(row["category"]),
            "description": str(row["description"]),
            "would_pay_signal": bool(row["would_pay_signal"]),
            "impact_level": str(row["impact_level"]),
            "cluster_size": int(row["cluster_size"]),
        }
    return None


def prepare_clusters(
    db: DuckDBResource,
    date: str,
    cluster_ids: List[int],
    embedding: "EmbeddingResource",
) -> Tuple[Dict[int, List[str]], Dict[int, str], Dict[int, str]]:
    """Get representative titles and clean_body snippets for each cluster using stored embeddings."""
    import numpy as np
    import pandas as pd
    from burningdemand.utils.cluster_representatives import get_cluster_representatives

    titles_by_cluster = {}
    snippets_by_cluster = {}

    for cid in cluster_ids:
        # Get items with embeddings for this cluster
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

        # Get embeddings from stored data
        embeddings_array = np.array(
            cluster_data["embedding"].tolist(), dtype=np.float32
        )

        # Prepare items DataFrame for representatives
        cluster_items = pd.DataFrame(
            {
                "title": cluster_data["title"],
                "body": cluster_data["body"],
                "source": cluster_data["source"],
            }
        )

        # Get representatives (will clean body internally)
        titles, snippets = get_cluster_representatives(
            cluster_items, embeddings_array, k=5
        )

        titles_by_cluster[cid] = titles
        snippets_by_cluster[cid] = snippets

    return titles_by_cluster, snippets_by_cluster, {}


async def label_cluster_with_llm(
    cid: int,
    size: int,
    authority_score: float,
    titles: List[str],
    snippets: str,
    fingerprint: str,
    date: str,
    db: DuckDBResource,
    llm: LLMResource,
    sem: asyncio.Semaphore,
    results: List[dict],
    failed: List[dict],
    errors: List[str],
) -> None:
    """Label a single cluster using LLM with retry and validation."""
    async with sem:
        # Check for existing label by fingerprint
        existing_label = get_existing_label_by_fingerprint(db, fingerprint)
        if existing_label:
            results.append(
                {
                    "cluster_date": date,
                    "cluster_id": int(cid),
                    "cluster_fingerprint": fingerprint,
                    "canonical_title": existing_label["canonical_title"],
                    "category": existing_label["category"],
                    "description": existing_label["description"],
                    "would_pay_signal": existing_label["would_pay_signal"],
                    "impact_level": existing_label["impact_level"],
                    "cluster_size": int(size),
                    "authority_score": float(authority_score),
                    "label_failed": False,
                }
            )
            return

        titles_str = "\n".join([f"- {t}" for t in titles if t])
        snippets_section = f"\n\nSnippets:\n{snippets}" if snippets else ""

        prompt = f"""Analyze these {len(titles)} issues (sampled from a cluster of {size}):

Titles:
{titles_str}{snippets_section}

Return ONLY valid JSON matching this exact schema:
{{
  "canonical_title": "Generic problem (max 120 chars)",
  "category": "ai|finance|compliance|logistics|healthtech|devtools|ecommerce|other",
  "description": "Root problem in 1-2 sentences",
  "would_pay_signal": true or false,
  "impact_level": "low|medium|high"
}}"""

        # Try local model first, fallback to remote on failure/large clusters
        models_to_try = []
        if size > 50:  # Large clusters: try remote first
            models_to_try = [llm.model, "groq/llama-3.1-70b-versatile"]
        else:  # Small clusters: try local first
            # Try local model if available (e.g., ollama), fallback to remote
            local_model = os.getenv("LOCAL_LLM_MODEL", None)
            if local_model:
                models_to_try = [local_model, llm.model]
            else:
                models_to_try = [llm.model]

        label_data = None
        last_error = None

        for model in models_to_try:
            for attempt in range(2):  # Retry once
                try:
                    response = await acompletion(
                        model=model,
                        messages=[{"role": "user", "content": prompt}],
                        max_tokens=500,
                        response_format={"type": "json_object"},  # Enforce JSON
                    )
                    raw = response.choices[0].message.content
                    data = extract_first_json_obj(raw)

                    # Validate with Pydantic
                    label = IssueLabel.model_validate(data)
                    label_data = {
                        "canonical_title": label.canonical_title,
                        "category": label.category,
                        "description": label.description,
                        "would_pay_signal": bool(label.would_pay_signal),
                        "impact_level": label.impact_level,
                    }
                    break  # Success
                except Exception as e:
                    last_error = e
                    if attempt == 0:
                        continue  # Retry once
                    else:
                        break  # Move to next model

            if label_data:
                break  # Success, exit model loop

        if label_data:
            results.append(
                {
                    "cluster_date": date,
                    "cluster_id": int(cid),
                    "cluster_fingerprint": fingerprint,
                    "canonical_title": label_data["canonical_title"],
                    "category": label_data["category"],
                    "description": label_data["description"],
                    "would_pay_signal": label_data["would_pay_signal"],
                    "impact_level": label_data["impact_level"],
                    "cluster_size": int(size),
                    "authority_score": float(authority_score),
                    "label_failed": False,
                }
            )
        else:
            # Mark as failed
            failed.append(
                {
                    "cluster_date": date,
                    "cluster_id": int(cid),
                    "cluster_fingerprint": fingerprint,
                    "label_failed": True,
                }
            )
            errors.append(f"cluster_id={cid}: Failed after retries: {last_error}")


def save_results(
    db: DuckDBResource, results: List[dict], failed: List[dict], date: str
) -> None:
    """Save labeled issues, failed labels, and evidence to database."""
    if results:
        df = pd.DataFrame(results)
        db.upsert_df(
            "gold",
            "issues",
            df,
            [
                "cluster_date",
                "cluster_id",
                "cluster_fingerprint",
                "canonical_title",
                "category",
                "description",
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
                "cluster_fingerprint",
                "label_failed",
            ],
        )

        # Evidence denorm in SQL (fast)
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


@asset(
    partitions_def=daily_partitions,
    group_name="gold",
    deps=["clusters"],
    description="Label clusters using LLM to extract canonical titles, categories, descriptions, and impact levels. Processes clusters ranked by authority_score (highest first) to prioritize high-engagement signals. Computes representatives on-the-fly, reuses labels via fingerprint, and includes LLM hardening with retry/validation.",
)
async def issues(
    context: AssetExecutionContext,
    db: DuckDBResource,
    llm: LLMResource,
    embedding: EmbeddingResource,
) -> MaterializeResult:
    date = context.partition_key

    unlabeled = get_unlabeled_clusters(db, date)

    if len(unlabeled) == 0:
        return MaterializeResult(metadata={"labeled": 0})

    cluster_ids = unlabeled["cluster_id"].astype(int).tolist()
    titles_by_cluster, snippets_by_cluster, _ = prepare_clusters(
        db, date, cluster_ids, embedding
    )

    # Compute fingerprints for all clusters
    fingerprints_by_cluster = {}
    for cid in cluster_ids:
        titles = titles_by_cluster.get(cid, [])
        fingerprints_by_cluster[cid] = compute_cluster_fingerprint(titles, k=5)

    sem = asyncio.Semaphore(8)
    errors = []
    results = []
    failed = []

    await asyncio.gather(
        *[
            label_cluster_with_llm(
                int(r["cluster_id"]),
                int(r["cluster_size"]),
                float(r.get("authority_score", 0.0)),
                titles_by_cluster.get(int(r["cluster_id"]), [])[:5],
                snippets_by_cluster.get(int(r["cluster_id"]), ""),
                fingerprints_by_cluster.get(int(r["cluster_id"]), ""),
                date,
                db,
                llm,
                sem,
                results,
                failed,
                errors,
            )
            for _, r in unlabeled.iterrows()
        ]
    )

    if results or failed:
        save_results(db, results, failed, date)

    md = {
        "attempted": int(len(unlabeled)),
        "labeled": int(len(results)),
        "failed": int(len(failed)),
        "errors": int(len(errors)),
    }
    if errors:
        md["error_samples"] = MetadataValue.json(errors[:5])

    return MaterializeResult(metadata=md)
