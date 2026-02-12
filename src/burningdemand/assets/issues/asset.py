import asyncio
from typing import List

from dagster import (
    AssetExecutionContext,
    AutoMaterializePolicy,
    MaterializeResult,
    MetadataValue,
    asset,
)
from burningdemand.utils.config import config
from burningdemand.partitions import daily_partitions
from burningdemand.resources.duckdb_resource import DuckDBResource
from .queries import clear_issues_data_for_date, get_groups_for_date, prepare_groups
from .labeling import label_group_with_llm
from .persist import save_results


@asset(
    partitions_def=daily_partitions,
    group_name="gold",
    deps=["groups"],
    description="Label groups using LLM to extract canonical titles, categories, descriptions, and impact levels. Processes groups ranked by authority_score (highest first) to prioritize high-engagement signals. Computes representatives on-the-fly and includes LLM retry/validation.",
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
async def issues(
    context: AssetExecutionContext,
    db: DuckDBResource,
) -> MaterializeResult:
    date = context.partition_key

    clear_issues_data_for_date(db, date)
    groups_df = get_groups_for_date(db, date)
    if len(groups_df) == 0:
        return MaterializeResult(metadata={"labeled": 0, "attempted": 0})

    group_ids = groups_df["group_id"].astype(int).tolist()
    titles_by_group, snippets_by_group = prepare_groups(db, date, group_ids)
    results: List[dict] = []
    failed: List[dict] = []
    errors: List[str] = []
    sem = asyncio.Semaphore(config.issues.llm_concurrency_per_issues_partition)

    await asyncio.gather(
        *[
            label_group_with_llm(
                int(r["group_id"]),
                int(r["group_size"]),
                float(r.get("authority_score", 0.0)),
                titles_by_group.get(int(r["group_id"]), []),
                snippets_by_group.get(int(r["group_id"]), ""),
                date,
                sem,
                results,
                failed,
                errors,
            )
            for _, r in groups_df.iterrows()
        ]
    )

    if results or failed:
        save_results(db, results, failed, date)

    metadata = {
        "attempted": int(len(groups_df)),
        "labeled": int(len(results)),
        "failed": int(len(failed)),
        "errors": int(len(errors)),
    }
    if errors:
        metadata["error_samples"] = MetadataValue.json(errors[:5])

    return MaterializeResult(metadata=metadata)
