"""Raw GitHub pull requests asset."""

from typing import Any, Dict

from dagster import AssetExecutionContext, MaterializeResult, asset

from burningdemand.partitions import daily_partitions
from burningdemand.resources.duckdb_resource import DuckDBResource
from burningdemand.resources.github_resource import GitHubResource
from burningdemand.schema.raw_items import RawItem
from burningdemand.utils.config import config
from burningdemand.utils.raw_utils import (
    materialize_raw,
    parse_github_comments_list,
    parse_github_labels,
    parse_github_reaction_groups,
)


def _pr_to_raw_item(d: Dict[str, Any]) -> RawItem:
    repository = d.get("repository") or {}
    repo = repository.get("nameWithOwner") or ""
    org, product = (repo.split("/", 1) + [""])[:2]
    license_info = repository.get("licenseInfo") or {}
    license_name = license_info.get("spdxId") or ""
    body = d.get("body") or ""
    meta = f"Pull Request: [state={d.get('state')}, merged={d.get('merged')}] \n"
    return RawItem(
        url=d.get("url") or "",
        title=d.get("title") or "",
        body=f"{meta}{body}".strip(),
        created_at=d.get("createdAt") or "",
        org_name=org,
        product_name=product,
        product_stars=repository.get("stargazerCount") or 0,
        product_forks=repository.get("forkCount") or 0,
        product_watchers=(repository.get("watchers") or {}).get("totalCount") or 0,
        source_post_id=str(d.get("id") or ""),
        license=license_name,
        comments_list=parse_github_comments_list(d.get("comments")),
        comments_count=((d.get("comments") or {}).get("totalCount") or 0),
        reactions_groups=parse_github_reaction_groups(d.get("reactionGroups") or []),
        reactions_count=((d.get("reactions") or {}).get("totalCount") or 0),
        upvotes_count=0,
        post_type="pull_request",
        labels=parse_github_labels(d.get("labels")),
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
            labels(first:{cfg.max_labels}) {{ nodes {{ name }} }}
            repository {{ 
                nameWithOwner
                description
                licenseInfo {{ spdxId }}
                stargazerCount
                forkCount
                watchers {{ totalCount }}
            }} 
            comments(last: {cfg.max_comments}) {{
                totalCount 
                nodes {{
                    body
                    updatedAt
                    reactionGroups {{ content reactors {{ totalCount }} }}
                }}
            }} 
            reactions {{ totalCount }}
            reactionGroups {{ content reactors {{ totalCount }} }}
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
        max_parallel=cfg.max_parallel,
    )

    wontfix_suffix = "is:pr sort:updated-desc label:wontfix reason:not_planned"
    wontfix_items, wontfix_meta = await github.search(
        date,
        node_fragment,
        type="ISSUE",
        query_suffix=wontfix_suffix,
        hour_splits=cfg.queries_per_day,
        per_page=cfg.per_page,
        max_parallel=cfg.max_parallel,
    )

    # Merge, deduplicating by id
    by_id: dict[str, Dict[str, Any]] = {d["id"]: d for d in raw_items}
    for d in wontfix_items:
        by_id.setdefault(d["id"], d)

    all_items = list(by_id.values())
    items = [_pr_to_raw_item(d) for d in all_items]
    meta = {
        "requests": int(meta.get("requests", 0)) + int(wontfix_meta.get("requests", 0)),
        "ok": int(meta.get("ok", 0)) + int(wontfix_meta.get("ok", 0)),
    }
    return await materialize_raw(db, items, meta, "gh_pull_requests", date)
