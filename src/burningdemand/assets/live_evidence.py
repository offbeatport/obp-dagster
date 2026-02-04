# burningdemand_dagster/assets/live_evidence.py
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

_SOURCE_TYPE_MAP = {
    "github": "github_issue",
    "stackoverflow": "stackoverflow_question",
    "reddit": "reddit_thread",
    "hackernews": "other",
}


@asset(
    partitions_def=daily_partitions,
    group_name="gold",
    deps=["live_issues"],
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    description="Sync issue evidence from gold.issue_evidence to PocketBase. Creates evidence records linked to issues synced by live_issues.",
)
async def live_evidence(
    context: AssetExecutionContext,
    db: DuckDBResource,
    pb: PocketBaseResource,
) -> MaterializeResult:
    date = context.partition_key

    evidence_rows = db.query_df(
        """
        SELECT cluster_id, source, url, body, posted_at
        FROM gold.issue_evidence
        WHERE cluster_date = ?
        """,
        [date],
    )

    if len(evidence_rows) == 0:
        return MaterializeResult(metadata={"posted": 0, "skipped": 0, "errors": 0})

    loop = asyncio.get_event_loop()

    def check_issue(pb_client, filter_expr):
        return pb_client.get_one_by_filter("issues", filter_expr)

    def check_evidence(pb_client, filter_expr):
        return pb_client.get_one_by_filter("evidence", filter_expr)

    def create_evidence(pb_client, payload):
        return pb_client.create("evidence", payload)

    date_escaped = str(date).replace('"', '\\"')
    posted = 0
    skipped = 0
    errors = 0
    issue_id_cache = {}

    for _, ev in evidence_rows.iterrows():
        cluster_id = int(ev["cluster_id"])
        ev_url = str(ev["url"])
        ev_source = str(ev.get("source", ""))
        ev_body = str(ev.get("body", ""))[:500]
        ev_posted_at = str(ev.get("posted_at", ""))

        try:
            if cluster_id not in issue_id_cache:
                filter_expr = f'origin="collected" && cluster_date="{date_escaped} 00:00:00.000Z" && cluster_id={cluster_id}'
                existing_issue = await loop.run_in_executor(
                    None,
                    check_issue,
                    pb,
                    filter_expr,
                )
                if not existing_issue:
                    context.log.warning(
                        f"No PocketBase issue for cluster_date={date} cluster_id={cluster_id}; skipping evidence"
                    )
                    issue_id_cache[cluster_id] = None
                    errors += 1
                    continue
                issue_id_cache[cluster_id] = existing_issue["id"]

            issue_id = issue_id_cache[cluster_id]
            if issue_id is None:
                errors += 1
                continue
            ev_url_escaped = ev_url.replace('"', '\\"')
            evidence_filter = f'issue="{issue_id}" && url="{ev_url_escaped}"'
            existing_evidence = await loop.run_in_executor(
                None,
                check_evidence,
                pb,
                evidence_filter,
            )

            if existing_evidence:
                skipped += 1
                continue

            evidence_payload = {
                "issue": issue_id,
                "url": ev_url,
                "source_type": _SOURCE_TYPE_MAP.get(ev_source, "other"),
                "content_snippet": ev_body,
                "posted_at": ev_posted_at,
            }
            await loop.run_in_executor(
                None,
                create_evidence,
                pb,
                evidence_payload,
            )
            posted += 1
        except Exception as e:
            context.log.warning(
                f"Failed to create evidence for cluster_id={cluster_id} url={ev_url}: {e}"
            )
            errors += 1

    return MaterializeResult(
        metadata={
            "posted": int(posted),
            "skipped": int(skipped),
            "errors": int(errors),
            "rows_considered": int(len(evidence_rows)),
        }
    )
