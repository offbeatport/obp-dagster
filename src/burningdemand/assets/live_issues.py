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

    # Load all labeled issues for this partition; we check PocketBase
    # to decide which need syncing, so PB is the source of truth.
    issues = db.query_df(
        """
        SELECT g.*
        FROM gold.issues g
        WHERE g.cluster_date = ?
        """,
        [date],
    )

    if len(issues) == 0:
        return MaterializeResult(metadata={"synced": 0})

    synced = 0
    skipped = 0

    loop = asyncio.get_event_loop()

    def check_issue(pb_client, filter_expr):
        return pb_client.get_one_by_filter("issues", filter_expr)

    def create_issue(pb_client, payload):
        return pb_client.create("issues", payload)

    for _, issue in issues.iterrows():

        cluster_id = int(issue["cluster_id"])
        issue_title = str(issue["canonical_title"])
        issue_cat = str(issue["category"]).split(",")
        desc_problem = str(issue.get("desc_problem", "") or "")
        desc_current_solutions = str(issue.get("desc_current_solutions", "") or "")
        desc_impact = str(issue.get("desc_impact", "") or "")
        desc_details = str(issue.get("desc_details", "") or "")
        date_escaped = str(date).replace('"', '\\"')
        filter_expr = f'origin="collected" && cluster_date="{date_escaped} 00:00:00.000Z" && cluster_id={cluster_id}'
        existing_issue = await loop.run_in_executor(
            None,
            check_issue,
            pb,
            filter_expr,
        )

        if existing_issue:
            skipped += 1
        else:
            try:
                issue_payload = {
                    "title": issue_title,
                    "desc_problem": desc_problem,
                    "desc_current_solutions": desc_current_solutions,
                    "desc_impact": desc_impact,
                    "desc_details": desc_details,
                    "category": issue_cat,
                    "status": "open",
                    "origin": "collected",
                    "reporter": pb.user_id,
                    "cluster_date": date,
                    "cluster_id": cluster_id,
                }
                await loop.run_in_executor(
                    None,
                    create_issue,
                    pb,
                    issue_payload,
                )
            except Exception as e:
                context.log.warning(
                    f"Failed to create issue for cluster {cluster_id}: {e}"
                )
                continue

        synced += 1

    return MaterializeResult(
        metadata={
            "synced": int(synced - skipped),
            "skipped": int(skipped),
            "issues_considered": int(len(issues)),
        }
    )
