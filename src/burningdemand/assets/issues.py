# burningdemand_dagster/assets/issues.py
import asyncio
import pandas as pd
import anthropic
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from burningdemand.partitions import daily_partitions
from burningdemand.resources.anthropic_resource import AnthropicResource
from burningdemand.resources.duckdb_resource import DuckDBResource
from burningdemand.utils.llm_schema import IssueLabel, extract_first_json_obj
from burningdemand.assets.clusters import clusters


@asset(partitions_def=daily_partitions, group_name="gold", deps=["clusters"])
async def issues(
    context: AssetExecutionContext,
    db: DuckDBResource,
    anthropic_api: AnthropicResource,
) -> MaterializeResult:
    date = context.partition_key
    client = anthropic.AsyncAnthropic(api_key=anthropic_api.api_key)

    unlabeled = db.query_df(
        """
        SELECT sc.cluster_id, sc.cluster_size
        FROM clusters sc
        WHERE sc.cluster_date = ?
          AND NOT EXISTS (
              SELECT 1 FROM issues gi
              WHERE gi.cluster_date = sc.cluster_date
                AND gi.cluster_id = sc.cluster_id
          )
        """,
        [date],
    )

    if len(unlabeled) == 0:
        return MaterializeResult(metadata={"labeled": 0})

    cluster_ids = unlabeled["cluster_id"].astype(int).tolist()

    titles_df = db.query_df(
        f"""
        SELECT cm.cluster_id, b.title
        FROM cluster_members cm
        JOIN embeddings s ON cm.url_hash = s.url_hash
        JOIN raw_items b ON s.url_hash = b.url_hash
        WHERE cm.cluster_date = ?
          AND cm.cluster_id IN ({",".join(["?"] * len(cluster_ids))})
        """,
        [date, *cluster_ids],
    )

    titles_by_cluster = {}
    for cid in cluster_ids:
        titles_by_cluster[cid] = (
            titles_df[titles_df["cluster_id"] == cid]["title"]
            .head(10)
            .fillna("")
            .astype(str)
            .tolist()
        )

    sem = asyncio.Semaphore(8)
    errors = []
    results = []

    async def label_one(cid: int, size: int):
        async with sem:
            titles = titles_by_cluster.get(cid, [])[:10]
            titles_str = "\n".join([f"- {t}" for t in titles if t])

            prompt = f"""Analyze these {len(titles)} issues (sampled from a cluster of {size}):

{titles_str}

Return ONLY valid JSON:
{{
  "canonical_title": "Generic problem (max 80 chars)",
  "category": "ai|finance|compliance|logistics|healthtech|devtools|ecommerce|other",
  "description": "Root problem in 1-2 sentences",
  "would_pay_signal": true/false,
  "impact_level": "low|medium|high"
}}"""

            try:
                msg = await client.messages.create(
                    model="claude-sonnet-4-20250514",
                    max_tokens=500,
                    messages=[{"role": "user", "content": prompt}],
                )
                raw = msg.content[0].text
                data = extract_first_json_obj(raw)
                label = IssueLabel.model_validate(data)

                results.append(
                    {
                        "cluster_date": date,
                        "cluster_id": int(cid),
                        "canonical_title": label.canonical_title,
                        "category": label.category,
                        "description": label.description,
                        "would_pay_signal": bool(label.would_pay_signal),
                        "impact_level": label.impact_level,
                        "cluster_size": int(size),
                    }
                )
            except Exception as e:
                errors.append(f"cluster_id={cid}: {type(e).__name__}: {e}")

    await asyncio.gather(
        *[
            label_one(int(r["cluster_id"]), int(r["cluster_size"]))
            for _, r in unlabeled.iterrows()
        ]
    )

    if results:
        df = pd.DataFrame(results)

        def _tx():
            db.insert_df(
                "issues",
                df,
                [
                    "cluster_date",
                    "cluster_id",
                    "canonical_title",
                    "category",
                    "description",
                    "would_pay_signal",
                    "impact_level",
                    "cluster_size",
                ],
            )

            # Evidence denorm in SQL (fast)
            db.execute(
                """
                INSERT INTO issue_evidence (cluster_date, cluster_id, url_hash, source, url, title, body, posted_at)
                SELECT cm.cluster_date,
                       cm.cluster_id,
                       s.url_hash,
                       b.source,
                       b.url,
                       b.title,
                       b.body,
                       b.created_at
                FROM cluster_members cm
                JOIN embeddings s
                  ON s.url_hash = cm.url_hash
                JOIN raw_items b
                  ON s.url_hash = b.url_hash
                WHERE cm.cluster_date = ?
                  AND EXISTS (
                      SELECT 1 FROM issues gi
                      WHERE gi.cluster_date = cm.cluster_date
                        AND gi.cluster_id = cm.cluster_id
                  )
                ON CONFLICT DO NOTHING
                """,
                [date],
            )

        db.transaction(_tx)

    md = {
        "attempted": int(len(unlabeled)),
        "labeled": int(len(results)),
        "errors": int(len(errors)),
    }
    if errors:
        md["error_samples"] = MetadataValue.json(errors[:5])

    return MaterializeResult(metadata=md)
