"""Raw GitHub issues asset."""

import pprint
from dagster import AssetExecutionContext, MaterializeResult, asset

from burningdemand.partitions import daily_partitions
from burningdemand.resources.duckdb_resource import DuckDBResource
from burningdemand.resources.github_resource import GitHubResource
from burningdemand.schema.raw_items import RawItem
from burningdemand.utils.config import config

from burningdemand.utils.raw_utils import materialize_raw
from typing import Any, Dict


def gh_to_raw_item(d: Dict[str, Any], post_type: str) -> RawItem:
    """Convert GitHub GQL node to RawItem."""
    url = d.get("url") or ""
    parts = url.rstrip("/").replace("https://github.com/", "").split("/")[:2]
    org, product = (parts + ["", ""])[:2]
    body = d.get("body") or ""
    created = d.get("createdAt") or ""
    source_id = str(d.get("id")) or ""
    comments = d.get("comments") or {}
    comments_list = [c.get("body") for c in comments.get("nodes", [])]
    comment_count = comments.get("totalCount") or 0
    reactions_groups = d.get("reactionGroups") or []
    reactions = d.get("reactions") or {}
    reactions_count = reactions.get("totalCount") or 0
    return RawItem(
        url=url,
        title=(d.get("title") or ""),
        body=body,
        org_name=org,
        product_name=product,
        comments_list=comments_list,
        comments_count=comment_count,
        votes_count=0,
        post_type=post_type,
        reactions_groups=reactions_groups,
        reactions_count=reactions_count,
        source_post_id=source_id,
        created_at=created,
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
    node_fragment = """
        ... on Issue { 
            id databaseId url title body createdAt 
            repository { nameWithOwner } 
            comments(last: 100) {
                totalCount 
                nodes {
                    body
                    updatedAt
                    reactionGroups {
                        content        
                        users {
                            totalCount
                        }
                    }
                    reactions {
                        totalCount
                    }  
                }
            } 
            reactionGroups {
                content        
                users {
                    totalCount
                }
            }
            reactions {
                totalCount
            }  
        }
    """
    cfg = config.raw_gh_issues
    query_suffix = (
        f"is:issue comments:>={cfg.min_comments} " f"reactions:>={cfg.min_reactions}"
    )
    raw_items, meta = await github.search(
        date,
        node_fragment,
        type="ISSUE",
        query_suffix=query_suffix,
        hour_splits=cfg.queries_per_day,
    )
    items = [
        item
        for item in (gh_to_raw_item(d, "issue") for d in raw_items)
        if item is not None
    ]
    pprint.pprint(items)
    return await materialize_raw(db, items, meta, "gh_issues", date)
