# burningdemand_dagster/assets/live_issues.py
import asyncio
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    AutoMaterializePolicy,
    asset,
)

from burningdemand.partitions import daily_partitions
from burningdemand.resources.duckdb_resource import DuckDBResource
from burningdemand.resources.pocketbase_resource import PocketBaseResource

# Max concurrent PocketBase creates
CREATE_CONCURRENCY = 5


@asset(
    partitions_def=daily_partitions,
    group_name="gold",
    deps=["issues"],
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    description="Sync labeled issues to PocketBase for live access. Creates or updates issue records in the external system.",
)
async def live_issues(
    context: AssetExecutionContext,
    db: DuckDBResource,
    pb: PocketBaseResource,
) -> MaterializeResult:
    date = context.partition_key

    issues_df = db.query_df(
        """
        SELECT g.*
        FROM gold.issues g
        WHERE g.group_date = ?
        """,
        [date],
    )

    if len(issues_df) == 0:
        return MaterializeResult(metadata={"synced": 0, "skipped": 0})

    date_escaped = str(date).replace('"', '\\"')
    filter_expr = f'origin="collected" && group_date="{date_escaped} 00:00:00.000Z"'
    existing_pb = pb.get_records("issues", filter_expr, per_page=500)
    existing_by_group = {int(r["group_id"]): r["id"] for r in existing_pb}

    to_create = []
    for _, issue in issues_df.iterrows():
        group_id = int(issue["group_id"])
        if group_id in existing_by_group:
            continue
        to_create.append(
            {
                "title": str(issue["canonical_title"]),
                "desc_problem": str(issue.get("desc_problem") or ""),
                "desc_current_solutions": str(issue.get("desc_current_solutions") or ""),
                "desc_impact": str(issue.get("desc_impact") or ""),
                "desc_details": str(issue.get("desc_details") or ""),
                "category": str(issue["category"]).split(","),
                "status": "open",
                "origin": "collected",
                "reporter": pb.user_id,
                "group_date": date,
                "group_id": group_id,
            }
        )

    loop = asyncio.get_event_loop()
    sem = asyncio.Semaphore(CREATE_CONCURRENCY)

    async def create_one(payload: dict):
        async with sem:
            return await loop.run_in_executor(
                None,
                lambda p=payload: pb.create("issues", p),
            )

    task_list = [asyncio.create_task(create_one(p)) for p in to_create]
    created = 0
    for done in asyncio.as_completed(task_list):
        try:
            await done
            created += 1
        except Exception as e:
            context.log.warning("Failed to create PocketBase issue: %s", e)

    skipped = len(issues_df) - len(to_create)
    return MaterializeResult(
        metadata={
            "synced": int(created),
            "skipped": int(skipped),
            "issues_considered": int(len(issues_df)),
        }
    )
