"""Raw GitHub pull requests asset."""

from typing import Any, Dict, List

from dagster import AssetExecutionContext, MaterializeResult, asset

from burningdemand.partitions import daily_partitions
from burningdemand.resources.duckdb_resource import DuckDBResource
from burningdemand.resources.github_resource import GitHubResource
from burningdemand.schema.raw_items import RawItem, RawReactionsGroups
from burningdemand.utils.config import config
from burningdemand.utils.raw_utils import materialize_raw


def _parse_reaction_groups(groups: List[Dict[str, Any]]) -> List[RawReactionsGroups]:
    return [
        RawReactionsGroups(
            type=(rg.get("content") or ""),
            count=(rg.get("reactors") or {}).get("totalCount") or 0,
        )
        for rg in groups
    ]


def _pr_to_raw_item(d: Dict[str, Any]) -> RawItem:
    repository = d.get("repository") or {}
    repo = repository.get("nameWithOwner") or ""
    org, product = (repo.split("/", 1) + [""])[:2]
    license_info = repository.get("licenseInfo") or {}
    license_name = (
        license_info.get("spdxId")
        or license_info.get("key")
        or license_info.get("name")
        or ""
    )
    body = d.get("body") or ""
    stats = (
        f"\n\n[PR Metadata] state={d.get('state')}, merged={d.get('merged')}, "
        f"changed_files={d.get('changedFiles')}, additions={d.get('additions')}, "
        f"deletions={d.get('deletions')}, commits={(d.get('commits') or {}).get('totalCount', 0)}, "
        f"reviews={(d.get('reviews') or {}).get('totalCount', 0)}"
    )
    return RawItem(
        url=d.get("url") or "",
        title=d.get("title") or "",
        body=f"{body}{stats}".strip(),
        created_at=d.get("createdAt") or "",
        source_post_id=str(d.get("id") or ""),
        comments_count=((d.get("comments") or {}).get("totalCount") or 0),
        upvotes_count=0,
        post_type="pull_request",
        reactions_groups=_parse_reaction_groups(d.get("reactionGroups") or []),
        reactions_count=((d.get("reactions") or {}).get("totalCount") or 0),
        org_name=org,
        product_name=product,
        license=license_name,
    )


@asset(
    partitions_def=daily_partitions,
    group_name="bronze",
    description="Raw GitHub pull requests per day. Writes into bronze.raw_items (source=gh_pull_requests, post_type=pull_request).",
)
async def raw_gh_pull_requests(
    context: AssetExecutionContext,
    db: DuckDBResource,
    github: GitHubResource,
) -> MaterializeResult:
    date = context.partition_key
    cfg = config.raw_gh_pull_requests

    node_fragment = f"""
        ... on PullRequest {{
            id
            url
            title
            body
            createdAt
            state
            merged
            mergedAt
            additions
            deletions
            changedFiles
            comments(last: {cfg.max_comments}) {{
                totalCount
            }}
            commits {{ totalCount }}
            reviews {{ totalCount }}
            repository {{ 
                nameWithOwner
                licenseInfo {{ spdxId name }}
            }}
            reactions {{ totalCount }}
            reactionGroups {{
                content
                reactors {{ totalCount }}
            }}
        }}
    """

    # Main slice: PRs with enough discussion
    query_suffix = f"is:pr comments:>={cfg.min_comments} sort:updated-desc"
    raw_items, meta = await github.search(
        date,
        node_fragment,
        type="ISSUE",
        query_suffix=query_suffix,
        hour_splits=cfg.queries_per_day,
        per_page=cfg.per_page,
    )

    # Extra slice: PRs that are explicitly marked as "won't fix"/not planned,
    # even if they don't meet the comments threshold.
    wontfix_suffix = "is:pr sort:updated-desc label:wontfix reason:not_planned"
    wontfix_items, wontfix_meta = await github.search(
        date,
        node_fragment,
        type="ISSUE",
        query_suffix=wontfix_suffix,
        hour_splits=cfg.queries_per_day,
        per_page=cfg.per_page,
    )

    # Merge, deduplicating by id
    by_id: dict[str, Dict[str, Any]] = {d["id"]: d for d in raw_items}
    for d in wontfix_items:
        by_id.setdefault(d["id"], d)

    all_items = list(by_id.values())
    items = [_pr_to_raw_item(d) for d in all_items]
    return await materialize_raw(db, items, meta, "gh_pull_requests", date)

