"""Raw GitHub issues asset."""

from typing import Any, Dict, List
from dagster import AssetExecutionContext, MaterializeResult, asset

from burningdemand.partitions import daily_partitions
from burningdemand.resources.duckdb_resource import DuckDBResource
from burningdemand.resources.github_resource import GitHubResource
from burningdemand.schema.raw_items import (
    CollectedItems,
    RawComment,
    RawItem,
    RawReactionsGroups,
)
from burningdemand.utils.config import config


def _parse_reaction_groups(groups: List[Dict[str, Any]]) -> List[RawReactionsGroups]:
    return [
        RawReactionsGroups(
            type=(rg.get("content") or ""),
            count=(rg.get("users") or {}).get("totalCount") or 0,
        )
        for rg in groups
    ]


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
        comments_list=[
            RawComment(
                body=(c.get("body") or ""),
                created_at=(c.get("updatedAt") or c.get("createdAt") or ""),
                upvotes_count=(c.get("reactions") or {}).get("totalCount") or 0,
                reactions=_parse_reaction_groups(c.get("reactionGroups") or []),
            )
            for c in d.get("comments").get("nodes", [])
        ],
        comments_count=d.get("comments").get("totalCount") or 0,
        upvotes_count=0,
        post_type=post_type,
        reactions_groups=_parse_reaction_groups(d.get("reactionGroups") or []),
        reactions_count=(d.get("reactions") or {}).get("totalCount") or 0,
        source_post_id=str(d.get("id")) or "",
        created_at=d.get("createdAt") or "",
        license=license_name,
    )


async def materialize_raw(
    db: DuckDBResource,
    items: List[RawItem],
    meta: Dict[str, Any],
    source: str,
    date: str,
) -> MaterializeResult:
    """Upsert collected items into bronze.raw_items. Returns empty result if no items."""
    if not items:
        return MaterializeResult(
            metadata={
                "source": source,
                "date": date,
                "collected": 0,
                "insert_attempted": 0,
                "collector": meta,
            }
        )
    collected = CollectedItems(items, meta)
    df = collected.to_df(source, date)
    inserted_attempt = db.upsert_df("bronze", "raw_items", df)
    return MaterializeResult(
        metadata={
            "source": source,
            "date": date,
            "collected": len(df),
            "insert_attempted": inserted_attempt,
            "collector": meta,
        }
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
            comments(last: {cfg.max_comments}) {{
                totalCount 
                nodes {{
                    body
                    updatedAt
                    reactionGroups {{
                        content        
                        reactors {{
                            totalCount
                        }}
                    }}
                    reactions {{
                        totalCount
                    }}  
                }}
            }} 
            reactionGroups {{
                content        
                reactors {{
                    totalCount
                }}
            }}
            reactions {{
                totalCount
            }}    
        }}
    """
    query_suffix = f"is:issue comments:>={cfg.min_comments}"
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
