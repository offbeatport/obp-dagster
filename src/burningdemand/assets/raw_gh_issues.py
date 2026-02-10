"""Raw GitHub issues asset."""

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


def gh_to_raw_item(d: Dict[str, Any], post_type: str) -> RawItem:
    """Convert GitHub GQL node to RawItem."""
    repository = d.get("repository") or {}
    name_with_owner = repository.get("nameWithOwner") or ""
    org, product = (name_with_owner.split("/", 1) + [""])[:2]
    license_info = repository.get("licenseInfo") or {}
    license_name = license_info.get("spdxId") or ""

    return RawItem(
        url=d.get("url"),
        title=d.get("title") or "",
        body=d.get("body") or "",
        org_name=org,
        product_name=product,
        product_desc=repository.get("description") or "",
        product_stars=repository.get("stargazerCount") or 0,
        product_forks=repository.get("forkCount") or 0,
        product_watchers=(repository.get("watchers") or {}).get("totalCount") or 0,
        comments_list=parse_github_comments_list(d.get("comments")),
        comments_count=d.get("comments").get("totalCount") or 0,
        upvotes_count=0,
        post_type=post_type,
        reactions_groups=parse_github_reaction_groups(d.get("reactionGroups") or []),
        reactions_count=(d.get("reactions") or {}).get("totalCount") or 0,
        source_post_id=str(d.get("id")) or "",
        created_at=d.get("createdAt") or "",
        license=license_name,
        labels=parse_github_labels(d.get("labels")),
    )


@asset(
    partitions_def=daily_partitions,
    group_name="bronze",
    description="Raw GitHub issues per day. Writes into bronze.raw_items (source=gh_issues, post_type=issue).",
)
async def raw_gh_issues(
    context: AssetExecutionContext,
    db: DuckDBResource,
    github: GitHubResource,
) -> MaterializeResult:
    date = context.partition_key
    cfg = config.raw_gh_issues

    node_fragment = f"""
        ... on Issue {{  
            id databaseId url title body createdAt 
            repository {{ 
                nameWithOwner
                description
                licenseInfo {{ spdxId }}
                stargazerCount
                forkCount
                watchers {{ totalCount }}
            }} 
            labels(first:{cfg.max_labels}) {{ nodes {{ name }} }}
            comments(last: {cfg.max_comments}) {{
                totalCount 
                nodes {{
                    body
                    updatedAt
                    reactionGroups {{ content reactors {{ totalCount }} }}
                }}
            }} 
            reactionGroups {{ content reactors {{ totalCount }} }}
            reactions {{ totalCount }}    
        }}
    """
    query_suffix = f"is:issue comments:>={cfg.min_comments} reactions:>={cfg.min_reactions} sort:interactions-desc"
    raw_items, meta = await github.search(
        date,
        node_fragment,
        type="ISSUE",
        query_suffix=query_suffix,
        hour_splits=cfg.queries_per_day,
        per_page=cfg.per_page,
    )
    items = [
        item
        for item in (gh_to_raw_item(d, "issue") for d in raw_items)
        if item is not None
    ]
    return await materialize_raw(db, items, meta, "gh_issues", date)
