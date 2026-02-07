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
    "gh_issues": "github_issue",
    "gh_discussions": "github_discussion",
    "rd": "reddit_thread",
    "so": "stackoverflow_question",
    "hn": "other",
}

CREATE_CONCURRENCY = 5
FETCH_EXISTING_CONCURRENCY = 5


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

    date_escaped = str(date).replace('"', '\\"')
    filter_expr = f'origin="collected" && cluster_date="{date_escaped} 00:00:00.000Z"'
    existing_issues = pb.get_records("issues", filter_expr, per_page=500)
    cluster_id_to_pb_issue_id = {int(r["cluster_id"]): r["id"] for r in existing_issues}

    # Rows to consider: (cluster_id, url, body, posted_at, source_type); only if we have a PB issue
    rows_with_issue = []
    for _, ev in evidence_rows.iterrows():
        cid = int(ev["cluster_id"])
        if cid not in cluster_id_to_pb_issue_id:
            continue
        rows_with_issue.append(
            (
                cluster_id_to_pb_issue_id[cid],
                str(ev["url"]),
                str(ev.get("body", ""))[:500],
                str(ev.get("posted_at", "")),
                _SOURCE_TYPE_MAP.get(str(ev.get("source", "")), "other"),
            )
        )

    # Fetch existing evidence for all involved issue IDs (parallel)
    issue_ids = list({r[0] for r in rows_with_issue})
    loop = asyncio.get_event_loop()
    sem_fetch = asyncio.Semaphore(FETCH_EXISTING_CONCURRENCY)

    def fetch_evidence_for_issue(issue_id: str):
        return pb.get_records("evidence", f'issue="{issue_id}"', per_page=500)

    async def fetch_one(iid: str):
        async with sem_fetch:
            return await loop.run_in_executor(
                None,
                fetch_evidence_for_issue,
                iid,
            )

    existing_evidence_list = await asyncio.gather(
        *[fetch_one(iid) for iid in issue_ids]
    )
    existing_by_issue = dict(zip(issue_ids, existing_evidence_list))
    existing_pairs = set()
    for recs in existing_by_issue.values():
        for r in recs:
            existing_pairs.add((r["issue"], r["url"]))

    to_create = [
        (issue_id, url, body, posted_at, source_type)
        for (issue_id, url, body, posted_at, source_type) in rows_with_issue
        if (issue_id, url) not in existing_pairs
    ]

    sem_create = asyncio.Semaphore(CREATE_CONCURRENCY)

    async def create_one(payload: dict):
        async with sem_create:
            return await loop.run_in_executor(
                None,
                lambda p=payload: pb.create("evidence", p),
            )

    task_list = [
        asyncio.create_task(
            create_one(
                {
                    "issue": issue_id,
                    "url": url,
                    "source_type": source_type,
                    "content_snippet": body,
                    "posted_at": posted_at,
                }
            )
        )
        for (issue_id, url, body, posted_at, source_type) in to_create
    ]
    posted = 0
    errors = 0
    for done in asyncio.as_completed(task_list):
        try:
            await done
            posted += 1
        except Exception as e:
            context.log.warning("Failed to create evidence: %s", e)
            errors += 1

    skipped = len(rows_with_issue) - len(to_create)
    return MaterializeResult(
        metadata={
            "posted": int(posted),
            "skipped": int(skipped),
            "errors": int(errors),
            "rows_considered": int(len(evidence_rows)),
        }
    )
