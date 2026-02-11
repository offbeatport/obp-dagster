"""Raw GitHub discussions asset."""

from typing import Any, Dict

from dagster import AssetExecutionContext, MaterializeResult, asset

from burningdemand.partitions import daily_partitions
from burningdemand.resources.duckdb_resource import DuckDBResource
from burningdemand.resources.github_resource import GitHubResource
from burningdemand.schema.raw_items import RawComment, RawItem
from burningdemand.utils.config import config
from burningdemand.utils.raw_utils import (
    materialize_raw,
    parse_github_comments_list,
    parse_github_labels,
    parse_github_reaction_groups,
)


def gh_to_raw_item(d: Dict[str, Any]) -> RawItem:
    """Convert GitHub GQL node to RawItem."""
    repository = d.get("repository") or {}
    name_with_owner = repository.get("nameWithOwner") or ""
    org, product = (name_with_owner.split("/", 1) + [""])[:2]
    license_info = repository.get("licenseInfo") or {}
    license_name = license_info.get("spdxId") or ""

    comments = parse_github_comments_list(d.get("comments") or {})
    comments_count = (d.get("comments") or {}).get("totalCount") or 0

    answer = parse_github_comments_list({"nodes": [d.get("answer") or {}]})
    answer_with_prefix = [
        RawComment(
            body=f"This is the answer to the discussion: {a.body}",
            reactions=a.reactions,
        )
        for a in answer
    ]
    comments_list = [*answer_with_prefix, *comments]

    return RawItem(
        url=d.get("url"),
        title=d.get("title") or "",
        body=d.get("body") or "",
        created_at=d.get("createdAt") or "",
        org_name=org,
        product_name=product,
        product_stars=repository.get("stargazerCount") or 0,
        product_forks=repository.get("forkCount") or 0,
        product_watchers=(repository.get("watchers") or {}).get("totalCount") or 0,
        source_post_id=str(d.get("id")) or "",
        license=license_name,
        comments_list=comments_list,
        comments_count=comments_count,
        reactions_groups=parse_github_reaction_groups(d.get("reactionGroups") or []),
        reactions_count=(d.get("reactions") or {}).get("totalCount") or 0,
        upvotes_count=d.get("upvoteCount") or 0,
        post_type="discussion",
        labels=parse_github_labels(d.get("labels")),
    )


@asset(
    partitions_def=daily_partitions,
    group_name="bronze",
    description="Raw GitHub discussions per day. Writes into bronze.raw_items (source=gh_discussions, post_type=discussion).",
)
async def raw_gh_discussions(
    context: AssetExecutionContext,
    db: DuckDBResource,
    github: GitHubResource,
) -> MaterializeResult:

    date = context.partition_key
    cfg = config.raw_gh_discussions

    node_fragment = f"""
        ... on Discussion {{
            id              
            url 
            title 
            body 
            createdAt
            upvoteCount
            isAnswered
            closed
            answer {{  
                body
                updatedAt
                reactions {{ totalCount }}
                reactionGroups {{ content reactors {{ totalCount }} }} 
            }}
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

    query_suffix = f"comments:>={cfg.min_comments} sort:interactions-desc"

    raw_items, meta = await github.search(
        date,
        node_fragment,
        query_suffix=query_suffix,
        type="DISCUSSION",
        hour_splits=cfg.queries_per_day,
        per_page=cfg.per_page,
        max_parallel=cfg.max_parallel,
    )

    items = [item for item in (gh_to_raw_item(d) for d in raw_items)]

    return await materialize_raw(db, items, meta, "gh_discussions", date)
