import asyncio
from typing import List

from dagster import (
    AssetExecutionContext,
    AutoMaterializePolicy,
    MaterializeResult,
    MetadataValue,
    asset,
)
from burningdemand.config import LLM_CONCURRENCY_PER_ISSUES_PARTITION
from burningdemand.partitions import daily_partitions
from burningdemand.resources.duckdb_resource import DuckDBResource
from burningdemand.utils.issues_utils import (
    clear_issues_data_for_date,
    get_clusters_for_date,
    label_cluster_with_llm,
    prepare_clusters,
    save_results,
)


@asset(
    partitions_def=daily_partitions,
    group_name="gold",
    deps=["clusters"],
    description="Label clusters using LLM to extract canonical titles, categories, descriptions, and impact levels. Processes clusters ranked by authority_score (highest first) to prioritize high-engagement signals. Computes representatives on-the-fly and includes LLM retry/validation.",
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
async def issues(
    context: AssetExecutionContext,
    db: DuckDBResource,
) -> MaterializeResult:
    date = context.partition_key

    clear_issues_data_for_date(db, date)
    clusters = get_clusters_for_date(db, date)
    if len(clusters) == 0:
        return MaterializeResult(metadata={"labeled": 0, "attempted": 0})

    cluster_ids = clusters["cluster_id"].astype(int).tolist()
    titles_by_cluster, snippets_by_cluster = prepare_clusters(db, date, cluster_ids)
    results: List[dict] = []
    failed: List[dict] = []
    errors: List[str] = []
    sem = asyncio.Semaphore(LLM_CONCURRENCY_PER_ISSUES_PARTITION)

    await asyncio.gather(
        *[
            label_cluster_with_llm(
                int(r["cluster_id"]),
                int(r["cluster_size"]),
                float(r.get("authority_score", 0.0)),
                titles_by_cluster.get(int(r["cluster_id"]), []),
                snippets_by_cluster.get(int(r["cluster_id"]), ""),
                date,
                sem,
                results,
                failed,
                errors,
            )
            for _, r in clusters.iterrows()
        ]
    )

    if results or failed:
        save_results(db, results, failed, date)

    metadata = {
        "attempted": int(len(clusters)),
        "labeled": int(len(results)),
        "failed": int(len(failed)),
        "errors": int(len(errors)),
    }
    if errors:
        metadata["error_samples"] = MetadataValue.json(errors[:5])

    return MaterializeResult(metadata=metadata)
